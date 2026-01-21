"""
Data Filtering Functions

Functions for applying conditional filters to intraday data based on various criteria.
"""

import pandas as pd
from typing import Optional, List, Set
from datetime import date
from ..data_sources.calendar import get_previous_trading_day
from ..data_sources.economic_events import is_economic_event_date


def get_week_start(check_date: date | pd.Timestamp | str) -> date:
    """
    Calculate the Monday (week start) for a given date.
    
    Uses Monday as the start of the week (dayofweek: Monday=0, Sunday=6).
    This function ensures consistent week calculation across all filters.
    
    Args:
        check_date: Date to calculate week start for
        
    Returns:
        Monday date of the week containing the given date
    """
    if isinstance(check_date, str):
        check_date = pd.to_datetime(check_date)
    elif isinstance(check_date, date):
        check_date = pd.to_datetime(check_date)
    
    days_since_monday = check_date.dayofweek
    week_start = check_date - pd.to_timedelta(days_since_monday, unit='D')
    return week_start.date()


def get_event_weeks(event_type: str) -> Set[date]:
    """
    Get all weeks (Monday dates) that contain a specific economic event.
    
    Note: This function uses static economic event dates from economic_events.py.
    No external API is required - dates are manually maintained in the codebase.
    For future extensibility, this could be updated to use an economic calendar API.
    
    Args:
        event_type: Type of economic event ('FOMC', 'CPI', 'NFP', etc.)
        
    Returns:
        Set of Monday dates (week_start) for weeks containing the event
        
    Raises:
        KeyError: If event_type is not found in economic events
    """
    from ..data_sources.economic_events import get_economic_event_dates
    
    event_dates = get_economic_event_dates(event_type)
    event_weeks = set()
    
    for event_date_str in event_dates:
        event_date = pd.to_datetime(event_date_str)
        week_start = get_week_start(event_date)
        event_weeks.add(week_start)
    
    return event_weeks


def trim_extremes(df: pd.DataFrame, lower_quantile: float = 0.05, upper_quantile: float = 0.95) -> pd.DataFrame:
    """
    Remove extreme values from the dataset based on quantiles.
    
    Args:
        df: DataFrame with 'pct_chg' and 'rng' columns
        lower_quantile: Lower quantile threshold (default 0.05 = bottom 5%)
        upper_quantile: Upper quantile threshold (default 0.95 = top 5%)
        
    Returns:
        Filtered DataFrame with extremes removed
    """
    if 'pct_chg' not in df.columns or 'rng' not in df.columns:
        return df
    
    low_pc, high_pc = df['pct_chg'].quantile([lower_quantile, upper_quantile])
    low_r, high_r = df['rng'].quantile([lower_quantile, upper_quantile])
    
    trimmed = df[
        df['pct_chg'].between(low_pc, high_pc) & 
        df['rng'].between(low_r, high_r)
    ]
    
    return trimmed if not trimmed.empty else df


def apply_filters(
    minute_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    filters: List[str],
    vol_threshold: Optional[float] = None,
    pct_threshold: Optional[float] = None
) -> pd.DataFrame:
    """
    Apply multiple conditional filters to minute data based on daily conditions.
    
    Args:
        minute_df: Minute-level OHLCV data
        daily_df: Daily OHLCV data with derived fields
        filters: List of filter names to apply
        vol_threshold: Relative volume threshold (e.g., 1.5 = 150% of average)
        pct_threshold: Percentage change threshold (e.g., 1.0 = 1%)
        
    Returns:
        Filtered minute DataFrame
        
    Available filters:
        - 'monday', 'tuesday', 'wednesday', 'thursday', 'friday': Day of week
        - 'prev_pos': Previous day closed higher
        - 'prev_neg': Previous day closed lower
        - 'prev_pct_pos': Previous day % change >= pct_threshold
        - 'prev_pct_neg': Previous day % change <= -pct_threshold
        - 'relvol_gt': Previous day relative volume > vol_threshold
        - 'relvol_lt': Previous day relative volume < vol_threshold
        - 'trim_extremes': Remove top/bottom 5% of returns and ranges
        - 'cpi_day': CPI release day
        - 'fomc_day': FOMC meeting day
        - 'fomc_week': All days in weeks that had FOMC meetings
        - 'nfp_day': Non-Farm Payrolls day
        - 'ppi_day': Producer Price Index day
        - 'retail_sales_day': Retail Sales release day
        - 'gdp_day': GDP release day
        - 'pce_day': PCE release day
        - 'major_event_day': Any major economic event day
    """
    # print(f"[DEBUG apply_filters] Input: {len(minute_df)} rows, filters={filters}")
    
    # Handle None or empty filters
    if filters is None:
        filters = []
    
    df = minute_df.copy()
    df['date'] = df['time'].dt.date
    
    # Add previous date using proper trading calendar - OPTIMIZED with vectorized mapping
    unique_dates = df['date'].unique()
    date_mapping = {date: get_previous_trading_day(date) for date in unique_dates}
    df['prev_date'] = df['date'].map(date_mapping)
    
    # Prepare daily data with previous day metrics
    daily_df = _prepare_daily_with_prev(daily_df)
    
    # Merge minute data with previous day information
    prev_cols = ['date', 'p_open', 'p_close', 'p_volume', 'p_volume_sma_10', 'p_return_pct']
    df = df.merge(
        daily_df[prev_cols],
        left_on='prev_date',
        right_on='date',
        how='left',
        suffixes=('', '_daily')
    )
    
    # Drop rows without previous day data
    df = df.dropna(subset=['p_open'])
    
    # Apply weekday filters
    weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
    selected_days = [f for f in filters if f in weekdays]
    
    if selected_days and set(selected_days) != set(weekdays):
        df = df[df['time'].dt.day_name().str.lower().isin(selected_days)]
    
    # Apply economic event filters
    # Map UI filter names (plural) to filter logic names (singular)
    economic_event_filters = {
        'cpi_day': 'CPI',
        'cpi_days': 'CPI',  # UI uses plural
        'fomc_day': 'FOMC',
        'fomc_days': 'FOMC',  # UI uses plural
        'nfp_day': 'NFP',
        'nfp_days': 'NFP',  # UI uses plural
        'ppi_day': 'PPI',
        'ppi_days': 'PPI',  # UI uses plural
        'retail_sales_day': 'RETAIL_SALES',
        'retail_sales_days': 'RETAIL_SALES',  # UI uses plural
        'gdp_day': 'GDP',
        'gdp_days': 'GDP',  # UI uses plural
        'pce_day': 'PCE',
        'pce_days': 'PCE',  # UI uses plural
    }
    
    # OPTIMIZED: Cache economic event dates and use vectorized operations (5-10x faster)
    for filter_name, event_type in economic_event_filters.items():
        if filter_name in filters:
            from ..data_sources.economic_events import get_economic_event_dates
            event_dates_str = get_economic_event_dates(event_type)
            event_dates = {pd.to_datetime(d).date() for d in event_dates_str}
            df = df[df['date'].isin(event_dates)]
    
    # Apply FOMC week filter (all days in weeks that had FOMC meetings) - OPTIMIZED
    if 'fomc_week' in filters:
        # Use helper function to get FOMC weeks (ensures consistent calculation)
        fomc_weeks = get_event_weeks('FOMC')
        
        # Filter dataframe to include only dates in weeks with FOMC meetings
        if fomc_weeks:
            # Convert date column to datetime for calculation - OPTIMIZED with vectorized mapping
            df_dt = pd.to_datetime(df['date'])
            unique_dates_dt = df_dt.unique()
            week_start_mapping = {d: get_week_start(d) for d in unique_dates_dt}
            df['week_start'] = df_dt.map(week_start_mapping)
            # Only keep rows where the week_start matches an FOMC week
            before_count = len(df)
            df = df[df['week_start'].isin(fomc_weeks)]
            after_count = len(df)
            # Debug output
            print(f"[FOMC WEEK FILTER] Found {len(fomc_weeks)} FOMC weeks. Filtered from {before_count} to {after_count} rows ({len(df['date'].unique())} unique dates)")
            df = df.drop(columns=['week_start'], errors='ignore')
        else:
            print(f"[FOMC WEEK FILTER] No FOMC weeks found - filter not applied")
    
    # Apply major event day filter (any economic event) - OPTIMIZED with vectorized operations
    if 'major_event_day' in filters:
        from ..data_sources.economic_events import get_all_major_event_dates
        major_dates_str = get_all_major_event_dates()
        major_dates = {pd.to_datetime(d).date() for d in major_dates_str}
        df = df[df['date'].isin(major_dates)]
    
    # Apply previous-day direction filters
    # Check for mutually exclusive filters
    if 'prev_pos' in filters and 'prev_neg' in filters:
        # Both filters are mutually exclusive with AND logic - warn but still apply (will result in 0 rows)
        import warnings
        warnings.warn("Both 'prev_pos' and 'prev_neg' filters are active with AND logic - these are mutually exclusive. Result will be 0 cases.")
        # Apply both - will result in empty dataframe (as expected)
        df = df[(df['p_close'] > df['p_open']) & (df['p_close'] < df['p_open'])]
    else:
        if 'prev_pos' in filters:
            df = df[df['p_close'] > df['p_open']]

        if 'prev_neg' in filters:
            df = df[df['p_close'] < df['p_open']]
    
    # Apply previous-day percentage change filters
    # Check for mutually exclusive percentage filters
    if 'prev_pct_pos' in filters and 'prev_pct_neg' in filters and pct_threshold is not None:
        # Both filters are mutually exclusive with AND logic - warn but still apply (will result in 0 rows)
        import warnings
        warnings.warn("Both 'prev_pct_pos' and 'prev_pct_neg' filters are active with AND logic at the same threshold - these are mutually exclusive. Result will be 0 cases.")
        # Apply both - will result in empty dataframe (as expected)
        df = df[(df['p_return_pct'] >= pct_threshold) & (df['p_return_pct'] <= -pct_threshold)]
    else:
        if 'prev_pct_pos' in filters and pct_threshold is not None:
            df = df[df['p_return_pct'] >= pct_threshold]
        
        if 'prev_pct_neg' in filters and pct_threshold is not None:
            df = df[df['p_return_pct'] <= -pct_threshold]
    
    # Apply relative volume filters
    df['p_relvol'] = df['p_volume'] / df['p_volume_sma_10']
    
    if 'relvol_gt' in filters and vol_threshold is not None:
        df = df[df['p_relvol'] > vol_threshold]
    
    if 'relvol_lt' in filters and vol_threshold is not None:
        df = df[df['p_relvol'] < vol_threshold]
    
    # Apply extreme trimming if requested
    if 'trim_extremes' in filters:
        df['pct_chg'] = (df['close'] - df['open']) / df['open']
        df['rng'] = df['high'] - df['low']
        df = trim_extremes(df)
    
    # print(f"[DEBUG apply_filters] Output: {len(df)} rows")
    return df


def _prepare_daily_with_prev(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare daily data with previous day metrics.
    
    Args:
        daily_df: Daily OHLCV DataFrame
        
    Returns:
        DataFrame with previous day columns prefixed with 'p_'
    """
    df = daily_df.copy()
    
    # Ensure we have required computed fields
    if 'volume_sma_10' not in df.columns:
        df['volume_sma_10'] = df['volume'].rolling(10, min_periods=1).mean()
    
    if 'day_return_pct' not in df.columns:
        df['day_return_pct'] = ((df['close'] - df['open']) / df['open']) * 100
    
    # Get previous day using proper trading calendar
    df['prev_date'] = df['date'].apply(lambda d: get_previous_trading_day(d))
    
    # Create a mapping from date to metrics
    date_index = df.set_index('date')
    
    # Map previous day metrics
    df['p_open'] = df['prev_date'].map(date_index['open'])
    df['p_close'] = df['prev_date'].map(date_index['close'])
    df['p_volume'] = df['prev_date'].map(date_index['volume'])
    df['p_volume_sma_10'] = df['prev_date'].map(date_index['volume_sma_10'])
    df['p_return_pct'] = df['prev_date'].map(date_index['day_return_pct'])
    
    return df


def apply_time_filters(
    df: pd.DataFrame,
    filters: List[str],
    time_a_hour: Optional[int] = None,
    time_a_minute: Optional[int] = None,
    time_b_hour: Optional[int] = None,
    time_b_minute: Optional[int] = None
) -> pd.DataFrame:
    """
    Apply time-based comparison filters (e.g., price at time A vs time B).
    
    Args:
        df: Minute-level DataFrame
        filters: List of filters ('timeA_gt_timeB', 'timeA_lt_timeB')
        time_a_hour: Hour for time A
        time_a_minute: Minute for time A
        time_b_hour: Hour for time B
        time_b_minute: Minute for time B
        
    Returns:
        Filtered DataFrame
    """
    if not any(f in filters for f in ['timeA_gt_timeB', 'timeA_lt_timeB']):
        return df
    
    if any(x is None for x in [time_a_hour, time_a_minute, time_b_hour, time_b_minute]):
        return df
    
    df = df.copy()
    df['date'] = df['time'].dt.date
    
    # Extract prices at specified times
    price_a = df[
        (df['time'].dt.hour == time_a_hour) & 
        (df['time'].dt.minute == time_a_minute)
    ].set_index('date')['close'].rename('price_a')
    
    price_b = df[
        (df['time'].dt.hour == time_b_hour) & 
        (df['time'].dt.minute == time_b_minute)
    ].set_index('date')['close'].rename('price_b')
    
    # Merge prices with main dataframe
    df = df.merge(price_a, left_on='date', right_index=True, how='left')
    df = df.merge(price_b, left_on='date', right_index=True, how='left')
    
    # Apply filters
    if 'timeA_gt_timeB' in filters:
        df = df[df['price_a'] > df['price_b']]
    
    if 'timeA_lt_timeB' in filters:
        df = df[df['price_a'] < df['price_b']]
    
    return df


def apply_percentage_change_zone_filters(
    minute_df: pd.DataFrame,
    enabled: bool,
    tolerance: Optional[float],
    start_day_offset: int,
    start_hour: int,
    start_minute: int,
    end_day_offset: int,
    end_hour: int,
    end_minute: int
) -> pd.DataFrame:
    """
    Filter minute data based on percentage change within a specific time zone.
    Keeps all minute data for days where the % change in the specified zone is within +/- tolerance.
    
    Args:
        minute_df: Minute-level OHLCV data
        enabled: Whether this filter is enabled
        tolerance: Tolerance percentage (e.g., 0.2 means +/- 0.2%)
        start_day_offset: Day offset for start time (-1 = T-1, 0 = T-0, 1 = T+1)
        start_hour: Start hour (0-23)
        start_minute: Start minute (0, 15, 30, 45)
        end_day_offset: Day offset for end time
        end_hour: End hour (0-23)
        end_minute: End minute (0, 15, 30, 45)
        
    Returns:
        Filtered minute DataFrame
    """
    if not enabled or tolerance is None:
        return minute_df
    
    df = minute_df.copy()
    df['date'] = df['time'].dt.date
    
    # Calculate percentage change for each day in the specified time zone
    from datetime import datetime
    from ..data_sources.calendar import get_previous_trading_day, get_next_trading_day
    
    def get_zone_pct_change(check_date: date) -> Optional[float]:
        """Calculate percentage change for a specific date's time zone."""
        try:
            # Calculate start and end dates based on offsets
            if start_day_offset == -1:
                start_date = get_previous_trading_day(check_date)
            elif start_day_offset == 0:
                start_date = check_date
            elif start_day_offset == 1:
                start_date = get_next_trading_day(check_date)
            else:
                return None
            
            if end_day_offset == -1:
                end_date = get_previous_trading_day(check_date)
            elif end_day_offset == 0:
                end_date = check_date
            elif end_day_offset == 1:
                end_date = get_next_trading_day(check_date)
            else:
                return None
            
            # Find minute data at start and end times
            start_mask = (df['time'].dt.date == start_date) & \
                        (df['time'].dt.hour == start_hour) & \
                        (df['time'].dt.minute == start_minute)
            end_mask = (df['time'].dt.date == end_date) & \
                      (df['time'].dt.hour == end_hour) & \
                      (df['time'].dt.minute == end_minute)
            
            start_data = df[start_mask]
            end_data = df[end_mask]
            
            if len(start_data) == 0 or len(end_data) == 0:
                return None
            
            start_price = start_data.iloc[0]['close']
            end_price = end_data.iloc[0]['close']
            
            if start_price == 0 or pd.isna(start_price) or pd.isna(end_price):
                return None
            
            pct_change = ((end_price - start_price) / start_price) * 100
            return pct_change
            
        except Exception:
            return None
    
    # Calculate zone % change for each unique date
    unique_dates = df['date'].unique()
    valid_dates = set()
    
    for check_date in unique_dates:
        zone_pct = get_zone_pct_change(check_date)
        if zone_pct is not None:
            # Keep if within tolerance: -tolerance <= zone_pct <= +tolerance
            if -tolerance <= zone_pct <= tolerance:
                valid_dates.add(check_date)
    
    # Filter: keep all minute data for dates where zone % change is within tolerance
    if valid_dates:
        df = df[df['date'].isin(valid_dates)]
    else:
        # No dates match, return empty dataframe
        df = df.iloc[0:0]
    
    return df

