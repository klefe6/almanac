"""
Zone-Based Percentage Change Filters

Production-grade filtering based on % change in specific time zones.
Supports cross-day windows with timezone-aware handling.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, List
from datetime import datetime, date, time, timedelta
import pytz

# New York timezone for session times
NY_TZ = pytz.timezone('America/New_York')


@dataclass
class ZoneFilterSpec:
    """Specification for a single zone filter."""
    name: str
    enabled: bool
    target_pct: float
    tolerance_pct: float
    start_day_offset: int  # -1, 0, 1
    start_hour: int        # 0-23
    start_minute: int      # 0-59
    end_day_offset: int
    end_hour: int
    end_minute: int
    
    def __post_init__(self):
        """Validate the spec."""
        if self.tolerance_pct < 0:
            raise ValueError(f"{self.name}: tolerance_pct must be >= 0")
        if not -1 <= self.start_day_offset <= 1:
            raise ValueError(f"{self.name}: day_offset must be in [-1, 0, 1]")
        if not -1 <= self.end_day_offset <= 1:
            raise ValueError(f"{self.name}: day_offset must be in [-1, 0, 1]")
        if not 0 <= self.start_hour <= 23:
            raise ValueError(f"{self.name}: hour must be in [0, 23]")
        if not 0 <= self.end_hour <= 23:
            raise ValueError(f"{self.name}: hour must be in [0, 23]")
        if not 0 <= self.start_minute <= 59:
            raise ValueError(f"{self.name}: minute must be in [0, 59]")
        if not 0 <= self.end_minute <= 59:
            raise ValueError(f"{self.name}: minute must be in [0, 59]")
    
    def get_range(self) -> Tuple[float, float]:
        """Get acceptable range [min, max] for percentage change."""
        return (
            self.target_pct - self.tolerance_pct,
            self.target_pct + self.tolerance_pct
        )
    
    def __repr__(self) -> str:
        min_pct, max_pct = self.get_range()
        return (f"ZoneFilter({self.name}, "
                f"target={self.target_pct:.2f}±{self.tolerance_pct:.2f}%, "
                f"range=[{min_pct:.2f}, {max_pct:.2f}]%, "
                f"start={self.start_day_offset};{self.start_hour:02d}:{self.start_minute:02d}, "
                f"end={self.end_day_offset};{self.end_hour:02d}:{self.end_minute:02d})")


def parse_zone_spec(
    name: str,
    enabled: bool,
    target_pct: Optional[float],
    tolerance_pct: Optional[float],
    start_day_offset: Optional[int],
    start_hour: Optional[int],
    start_minute: Optional[int],
    end_day_offset: Optional[int],
    end_hour: Optional[int],
    end_minute: Optional[int]
) -> Optional[ZoneFilterSpec]:
    """
    Parse and validate zone filter spec from UI inputs.
    
    Returns:
        ZoneFilterSpec if valid and enabled, None if disabled or invalid
    """
    if not enabled:
        return None
    
    # Check for missing required parameters
    if target_pct is None or tolerance_pct is None:
        raise ValueError(f"{name}: enabled but missing target_pct or tolerance_pct")
    
    # Use defaults if any time component is missing
    if start_day_offset is None:
        start_day_offset = 0
    if start_hour is None:
        start_hour = 9
    if start_minute is None:
        start_minute = 30
    if end_day_offset is None:
        end_day_offset = 0
    if end_hour is None:
        end_hour = 16
    if end_minute is None:
        end_minute = 0
    
    try:
        return ZoneFilterSpec(
            name=name,
            enabled=True,
            target_pct=float(target_pct),
            tolerance_pct=float(tolerance_pct),
            start_day_offset=int(start_day_offset),
            start_hour=int(start_hour),
            start_minute=int(start_minute),
            end_day_offset=int(end_day_offset),
            end_hour=int(end_hour),
            end_minute=int(end_minute)
        )
    except (ValueError, TypeError) as e:
        raise ValueError(f"{name}: invalid parameters - {e}")


def _ensure_timezone_aware(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the time column is timezone-aware (America/New_York).
    
    Args:
        df: DataFrame with 'time' column
        
    Returns:
        DataFrame with tz-aware 'time' column
    """
    df = df.copy()
    
    if pd.api.types.is_datetime64_any_dtype(df['time']):
        if df['time'].dt.tz is None:
            # Localize naive timestamps to NY time
            df['time'] = df['time'].dt.tz_localize(NY_TZ, ambiguous='infer', nonexistent='shift_forward')
        else:
            # Convert to NY time
            df['time'] = df['time'].dt.tz_convert(NY_TZ)
    else:
        raise TypeError("'time' column must be datetime type")
    
    return df


def _get_trading_date(dt: datetime) -> date:
    """
    Get the trading date for a given datetime.
    
    For times before 5am, consider them part of previous trading day.
    """
    if dt.hour < 5:
        return (dt - timedelta(days=1)).date()
    return dt.date()


def compute_zone_pct_change(
    minute_df: pd.DataFrame,
    analysis_date: date,
    spec: ZoneFilterSpec
) -> Tuple[Optional[float], str]:
    """
    Compute % change for a specific zone on a given analysis date.
    
    Args:
        minute_df: Minute OHLCV data with timezone-aware 'time' column
        analysis_date: The trading day we're analyzing (T-0)
        spec: Zone filter specification
        
    Returns:
        (pct_change, status_message)
        - pct_change is None if computation failed
        - status_message explains what happened
    """
    # Calculate actual calendar dates for start and end
    start_date = analysis_date + timedelta(days=spec.start_day_offset)
    end_date = analysis_date + timedelta(days=spec.end_day_offset)
    
    # Build target timestamps
    start_dt = NY_TZ.localize(datetime.combine(start_date, time(spec.start_hour, spec.start_minute)))
    end_dt = NY_TZ.localize(datetime.combine(end_date, time(spec.end_hour, spec.end_minute)))
    
    # Handle window crossing midnight (end < start means next day)
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)
    
    # Find bars in the time window
    mask = (minute_df['time'] >= start_dt) & (minute_df['time'] <= end_dt)
    window_data = minute_df[mask]
    
    if len(window_data) == 0:
        return None, f"no bars in window [{start_dt}, {end_dt}]"
    
    # Get first bar's open and last bar's close
    first_bar = window_data.iloc[0]
    last_bar = window_data.iloc[-1]
    
    open_price = first_bar['open']
    close_price = last_bar['close']
    
    if pd.isna(open_price) or pd.isna(close_price):
        return None, "missing open or close price"
    
    if open_price == 0:
        return None, "open price is zero"
    
    pct_change = ((close_price - open_price) / open_price) * 100
    
    if not np.isfinite(pct_change):
        return None, "non-finite % change"
    
    return pct_change, f"computed from {len(window_data)} bars"


def apply_zone_filters(
    minute_df: pd.DataFrame,
    specs: List[ZoneFilterSpec]
) -> Tuple[pd.DataFrame, Dict[str, any]]:
    """
    Apply multiple zone filters to minute data.
    
    Args:
        minute_df: Minute OHLCV data
        specs: List of enabled zone filter specifications
        
    Returns:
        (filtered_df, diagnostics)
        - filtered_df: Minute data for days passing all filters
        - diagnostics: Dict with detailed stats about filtering
    """
    if not specs:
        # No filters enabled, return all data
        return minute_df, {
            'total_days': minute_df['time'].dt.date.nunique() if len(minute_df) > 0 else 0,
            'days_remaining': minute_df['time'].dt.date.nunique() if len(minute_df) > 0 else 0,
            'filters_applied': [],
            'days_dropped': 0
        }
    
    # Ensure timezone-aware
    df = _ensure_timezone_aware(minute_df)
    
    # Add date column (using trading date logic)
    df['analysis_date'] = df['time'].apply(_get_trading_date)
    
    # Get all unique analysis dates
    all_dates = sorted(df['analysis_date'].unique())
    total_days = len(all_dates)
    
    # Track results per filter
    filter_results = {}
    days_passing_all = set(all_dates)
    
    for spec in specs:
        passing = set()
        failing = set()
        errors = {}
        
        for analysis_date in all_dates:
            pct_change, status = compute_zone_pct_change(df, analysis_date, spec)
            
            if pct_change is None:
                failing.add(analysis_date)
                errors[analysis_date] = status
            else:
                min_pct, max_pct = spec.get_range()
                if min_pct <= pct_change <= max_pct:
                    passing.add(analysis_date)
                else:
                    failing.add(analysis_date)
                    errors[analysis_date] = f"out of range: {pct_change:.2f}% not in [{min_pct:.2f}, {max_pct:.2f}]%"
        
        filter_results[spec.name] = {
            'spec': spec,
            'days_passing': len(passing),
            'days_failing': len(failing),
            'errors': errors
        }
        
        days_passing_all &= passing
    
    # Filter to only days passing all filters
    filtered_df = df[df['analysis_date'].isin(days_passing_all)].copy()
    filtered_df = filtered_df.drop(columns=['analysis_date'])
    
    # Build diagnostics
    diagnostics = {
        'total_days': total_days,
        'days_remaining': len(days_passing_all),
        'days_dropped': total_days - len(days_passing_all),
        'filters_applied': filter_results,
        'days_passing_all': sorted(list(days_passing_all))
    }
    
    return filtered_df, diagnostics


def format_diagnostics(diagnostics: Dict) -> List[str]:
    """
    Format diagnostics into human-readable strings.
    
    Args:
        diagnostics: Output from apply_zone_filters
        
    Returns:
        List of formatted strings for display
    """
    lines = []
    lines.append(f"📊 Zone Filter Results:")
    lines.append(f"Total days in range: {diagnostics['total_days']}")
    lines.append(f"Days remaining: {diagnostics['days_remaining']}")
    lines.append(f"Days dropped: {diagnostics['days_dropped']}")
    
    if not diagnostics['filters_applied']:
        lines.append("(No filters enabled)")
        return lines
    
    lines.append("")
    for filter_name, results in diagnostics['filters_applied'].items():
        spec = results['spec']
        min_pct, max_pct = spec.get_range()
        lines.append(f"Filter: {filter_name}")
        lines.append(f"  Target: {spec.target_pct:.2f}% ± {spec.tolerance_pct:.2f}% → [{min_pct:.2f}%, {max_pct:.2f}%]")
        lines.append(f"  Window: {spec.start_day_offset};{spec.start_hour:02d}:{spec.start_minute:02d} to "
                    f"{spec.end_day_offset};{spec.end_hour:02d}:{spec.end_minute:02d}")
        lines.append(f"  Days passing: {results['days_passing']} / {diagnostics['total_days']}")
        lines.append(f"  Days failing: {results['days_failing']}")
        
        # Show sample errors (up to 3)
        if results['errors']:
            error_samples = list(results['errors'].items())[:3]
            lines.append(f"  Sample failures:")
            for date, reason in error_samples:
                lines.append(f"    {date}: {reason}")
        lines.append("")
    
    return lines

