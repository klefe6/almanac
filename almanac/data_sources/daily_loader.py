"""
Daily Data Loader

Loads and processes daily OHLCV data from files or database.
"""

import pandas as pd
from typing import Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

try:
    from sqlalchemy import text
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

from .file_loader import load_daily_data_from_file


def load_daily_data(
    product: str,
    start_date: str | datetime,
    end_date: str | datetime,
    add_derived_fields: bool = True,
    use_files: bool = True
) -> pd.DataFrame:
    """
    Load daily OHLCV data from files or database.
    
    Tries file-based loading first, falls back to database if files not available.
    For TSLA, generates daily data from minute data via Alpaca API.
    For BTCUSD and other crypto, generates daily data from minute data via Yahoo Finance API.
    
    Args:
        product: Contract ID (e.g., 'ES', 'NQ', 'GC', 'TSLA', 'BTCUSD')
        start_date: Start date (YYYY-MM-DD or datetime)
        end_date: End date (YYYY-MM-DD or datetime)
        add_derived_fields: Whether to add derived fields (date, rolling volumes, etc.)
        use_files: Whether to try loading from files first (default True)
        
    Returns:
        DataFrame with columns: time, open, high, low, close, volume, date
        
    Raises:
        ValueError: If no data found
    """
    # Handle cryptocurrency data (BTCUSD, etc.) by loading from cached files or API
    try:
        from .yfinance_loader import is_crypto_symbol, load_crypto_minute_data
        if is_crypto_symbol(product):
            try:
                # First try to load from cached daily file
                if use_files:
                    try:
                        return load_daily_data_from_file(product, start_date, end_date, add_derived_fields)
                    except Exception as e:
                        logger.warning(f"Failed to load {product} daily from cache: {e}")
                
                # If no cached daily data, try to generate from minute data
                try:
                    # Load minute data and convert to daily
                    minute_df = load_crypto_minute_data(product, start_date, end_date, use_cache=True, validate=False)
                    
                    if minute_df.empty:
                        raise ValueError(f"No {product} data found for date range {start_date} to {end_date}")
                    
                    # Convert minute data to daily OHLCV
                    daily_df = _convert_minute_to_daily(minute_df)
                    
                    if add_derived_fields:
                        daily_df = _add_derived_fields(daily_df)
                    
                    return daily_df
                    
                except Exception as e:
                    raise ValueError(f"Failed to load {product} daily data: {e}")
                
            except Exception as e:
                raise ValueError(f"Failed to load {product} daily data: {e}")
    except ImportError:
        pass  # yfinance_loader not available, continue with normal flow
    
    # Handle TSLA data by loading from cached files
    if product == 'TSLA':
        try:
            # First try to load from cached daily file
            if use_files:
                try:
                    return load_daily_data_from_file(product, start_date, end_date, add_derived_fields)
                except Exception as e:
                    logger.warning(f"Failed to load TSLA daily from cache: {e}")
            
            # If no cached daily data, try to generate from minute data
            try:
                from .alpaca_loader import load_tsla_minute_data
                
                # Load minute data and convert to daily
                minute_df = load_tsla_minute_data(start_date, end_date, use_cache=True, save_cache=False)
                
                if minute_df.empty:
                    raise ValueError(f"No TSLA data found for date range {start_date} to {end_date}")
                
                # Convert minute data to daily OHLCV
                daily_df = _convert_minute_to_daily(minute_df)
                
                if add_derived_fields:
                    daily_df = _add_derived_fields(daily_df)
                
                return daily_df
                
            except ImportError:
                raise ValueError("TSLA data requires alpaca-py. Install with: pip install alpaca-py>=0.20.0")
            except Exception as e:
                raise ValueError(f"Failed to load TSLA daily data: {e}")
            
        except Exception as e:
            raise ValueError(f"Failed to load TSLA daily data: {e}")
    
    # Try file-based loading first for other products
    if use_files:
        try:
            return load_daily_data_from_file(product, start_date, end_date, add_derived_fields)
        except Exception as e:
            print(f"File loading failed, trying database: {e}")
    
    # Fallback to database
    if not HAS_SQLALCHEMY:
        raise ImportError(
            "SQLAlchemy is required for database fallback. "
            "Install it with: pip install sqlalchemy pyodbc"
        )
    
    from .db_config import get_engine
    engine = get_engine()
    
    # Convert dates to strings if needed
    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, datetime):
        end_date = end_date.strftime("%Y-%m-%d")
    
    sql = text("""
        SELECT [time], [open], [high], [low], [close], [volume]
        FROM dbo.DailyData
        WHERE contract_id = :prod
          AND [time] BETWEEN :start AND :end
        ORDER BY [time]
    """)
    
    df = pd.read_sql(
        sql,
        engine,
        params={
            'prod': product,
            'start': f"{start_date} 00:00:00",
            'end': f"{end_date} 23:59:59"
        },
        parse_dates=['time']
    )
    
    if df.empty:
        raise ValueError(
            f"No daily data found for {product} between {start_date} and {end_date}"
        )
    
    # Add date field
    df['date'] = df['time'].dt.date
    
    if add_derived_fields:
        df = _add_derived_fields(df)
    
    # Return only useful columns
    base_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    derived_cols = [c for c in df.columns if c not in ['time'] and c in df.columns]
    return df[derived_cols]


def _add_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add commonly used derived fields to daily data.
    
    Args:
        df: Raw daily dataframe
        
    Returns:
        DataFrame with additional computed fields
    """
    df = df.copy()
    
    # Direction
    df['is_green'] = df['close'] > df['open']
    df['is_red'] = df['close'] < df['open']
    
    # Returns
    df['day_return'] = (df['close'] - df['open']) / df['open']
    df['day_return_pct'] = df['day_return'] * 100
    
    # Range
    df['range'] = df['high'] - df['low']
    df['range_pct'] = (df['range'] / df['open']) * 100
    
    # Rolling volumes
    df['volume_sma_10'] = df['volume'].rolling(10, min_periods=1).mean()
    df['volume_sma_20'] = df['volume'].rolling(20, min_periods=1).mean()
    df['relative_volume'] = df['volume'] / df['volume_sma_10']
    
    # Rolling price metrics
    df['atr_14'] = df['range'].rolling(14, min_periods=1).mean()
    
    # Weekday
    df['weekday'] = df['date'].apply(lambda d: pd.Timestamp(d).day_name())
    df['weekday_num'] = df['date'].apply(lambda d: pd.Timestamp(d).weekday())
    
    return df


def get_daily_data_summary(
    product: str,
    start_date: str,
    end_date: str
) -> dict:
    """
    Get summary statistics about daily data.
    
    Args:
        product: Contract ID
        start_date: Start date
        end_date: End date
        
    Returns:
        Dictionary with summary stats
    """
    engine = get_engine()
    
    sql = text("""
        SELECT 
            COUNT(*) as total_days,
            MIN([time]) as first_date,
            MAX([time]) as last_date,
            AVG([volume]) as avg_volume,
            MIN([low]) as period_low,
            MAX([high]) as period_high
        FROM dbo.DailyData
        WHERE contract_id = :prod
          AND [time] BETWEEN :start AND :end
    """)
    
    result = pd.read_sql(
        sql,
        engine,
        params={
            'prod': product,
            'start': f"{start_date} 00:00:00",
            'end': f"{end_date} 23:59:59"
        }
    )
    
    return result.iloc[0].to_dict() if not result.empty else {}


def _convert_minute_to_daily(minute_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert minute-level data to daily OHLCV data.
    
    Args:
        minute_df: DataFrame with minute data (time, open, high, low, close, volume)
        
    Returns:
        DataFrame with daily data (time, open, high, low, close, volume)
    """
    if minute_df.empty:
        return pd.DataFrame()
    
    # Ensure time column is datetime
    minute_df = minute_df.copy()
    minute_df['time'] = pd.to_datetime(minute_df['time'])
    
    # Group by date and aggregate
    daily_df = minute_df.groupby(minute_df['time'].dt.date).agg({
        'open': 'first',    # First price of the day
        'high': 'max',      # Highest price of the day
        'low': 'min',       # Lowest price of the day
        'close': 'last',    # Last price of the day
        'volume': 'sum'     # Total volume for the day
    }).reset_index()
    
    # Rename date column to time for consistency
    daily_df = daily_df.rename(columns={'time': 'time'})
    
    # Convert date back to datetime
    daily_df['time'] = pd.to_datetime(daily_df['time'])
    
    # Ensure proper column order
    daily_df = daily_df[['time', 'open', 'high', 'low', 'close', 'volume']]
    
    return daily_df


def _add_derived_fields(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived fields to daily data.
    
    Args:
        daily_df: DataFrame with daily OHLCV data
        
    Returns:
        DataFrame with additional derived fields
    """
    if daily_df.empty:
        return daily_df
    
    df = daily_df.copy()
    
    # Add date field (date only, no time)
    df['date'] = df['time'].dt.date
    
    # Add price change fields
    df['price_change'] = df['close'] - df['open']
    df['price_change_pct'] = (df['price_change'] / df['open']) * 100
    
    # Add high-low range
    df['range'] = df['high'] - df['low']
    df['range_pct'] = (df['range'] / df['open']) * 100
    
    # Add rolling volume (5-day average)
    df['volume_5d_avg'] = df['volume'].rolling(window=5, min_periods=1).mean()
    
    # Add volume ratio (current volume / 5-day average)
    df['volume_ratio'] = df['volume'] / df['volume_5d_avg']
    
    return df

