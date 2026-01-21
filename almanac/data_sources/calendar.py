"""
Trading Calendar Utilities

Handles trading days, holidays, and session times using exchange calendars.
"""

import pandas as pd
from datetime import date, datetime, time
from typing import Optional
import warnings
import pytz

# Try to import pandas_market_calendars, fallback to BDay if not available
try:
    import pandas_market_calendars as mcal
    HAS_MARKET_CALENDARS = True
except ImportError:
    HAS_MARKET_CALENDARS = False
    warnings.warn(
        "pandas_market_calendars not installed. Using BDay approximation. "
        "Install with: pip install pandas-market-calendars",
        ImportWarning
    )


def get_exchange_calendar(exchange: str = "CME"):
    """
    Get trading calendar for specified exchange.
    
    Args:
        exchange: Exchange code (CME, CBOT, NYSE, etc.)
        
    Returns:
        Market calendar instance or None if not available
    """
    if not HAS_MARKET_CALENDARS:
        return None
    
    try:
        return mcal.get_calendar(exchange)
    except Exception:
        # Fallback to CME if specific exchange not found
        return mcal.get_calendar("CME")


def get_previous_trading_day(
    current_date: date | str | pd.Timestamp,
    exchange: str = "CME"
) -> date:
    """
    Get the previous trading day, accounting for holidays and weekends.
    
    Args:
        current_date: Reference date
        exchange: Exchange to use for trading calendar
        
    Returns:
        Previous trading day as date object
    """
    if isinstance(current_date, str):
        current_date = pd.Timestamp(current_date).date()
    elif isinstance(current_date, pd.Timestamp):
        current_date = current_date.date()
    
    if HAS_MARKET_CALENDARS:
        try:
            cal = get_exchange_calendar(exchange)
            # Get valid trading days up to current_date
            schedule = cal.valid_days(
                start_date=pd.Timestamp(current_date) - pd.Timedelta(days=10),
                end_date=pd.Timestamp(current_date)
            )
            
            # Find the previous trading day
            valid_dates = schedule[schedule < pd.Timestamp(current_date)]
            if len(valid_dates) > 0:
                return valid_dates[-1].date()
        except Exception:
            pass
    
    # Fallback: Use business day approximation
    prev = pd.Timestamp(current_date) - pd.tseries.offsets.BDay(1)
    return prev.date()


def get_next_trading_day(
    current_date: date | str | pd.Timestamp,
    exchange: str = "CME"
) -> date:
    """
    Get the next trading day, accounting for holidays and weekends.
    
    Args:
        current_date: Reference date
        exchange: Exchange to use for trading calendar
        
    Returns:
        Next trading day as date object
    """
    if isinstance(current_date, str):
        current_date = pd.Timestamp(current_date).date()
    elif isinstance(current_date, pd.Timestamp):
        current_date = current_date.date()
    
    if HAS_MARKET_CALENDARS:
        try:
            cal = get_exchange_calendar(exchange)
            # Get valid trading days after current_date
            schedule = cal.valid_days(
                start_date=pd.Timestamp(current_date),
                end_date=pd.Timestamp(current_date) + pd.Timedelta(days=10)
            )
            
            # Find the next trading day
            valid_dates = schedule[schedule > pd.Timestamp(current_date)]
            if len(valid_dates) > 0:
                return valid_dates[0].date()
        except Exception:
            pass
    
    # Fallback: Use business day approximation
    next_day = pd.Timestamp(current_date) + pd.tseries.offsets.BDay(1)
    return next_day.date()


def is_trading_day(check_date: date | str | pd.Timestamp, exchange: str = "CME") -> bool:
    """
    Check if a given date is a trading day.
    
    Args:
        check_date: Date to check
        exchange: Exchange to use for trading calendar
        
    Returns:
        True if trading day, False otherwise
    """
    if isinstance(check_date, str):
        check_date = pd.Timestamp(check_date)
    elif isinstance(check_date, date):
        check_date = pd.Timestamp(check_date)
    
    if HAS_MARKET_CALENDARS:
        try:
            cal = get_exchange_calendar(exchange)
            schedule = cal.valid_days(start_date=check_date, end_date=check_date)
            return len(schedule) > 0
        except Exception:
            pass
    
    # Fallback: Check if weekday
    return check_date.weekday() < 5


def get_trading_days(
    start_date: date | str | pd.Timestamp,
    end_date: date | str | pd.Timestamp,
    exchange: str = "CME"
) -> pd.DatetimeIndex:
    """
    Get all trading days in a date range.
    
    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        exchange: Exchange to use for trading calendar
        
    Returns:
        DatetimeIndex of trading days
    """
    if isinstance(start_date, str):
        start_date = pd.Timestamp(start_date)
    if isinstance(end_date, str):
        end_date = pd.Timestamp(end_date)
    
    if HAS_MARKET_CALENDARS:
        try:
            cal = get_exchange_calendar(exchange)
            return cal.valid_days(start_date=start_date, end_date=end_date)
        except Exception:
            pass
    
    # Fallback: Use business days
    return pd.bdate_range(start=start_date, end=end_date)


def get_product_trading_hours(product: str) -> dict:
    """
    Get trading hours for a specific product.
    
    Args:
        product: Product symbol (e.g., 'ES', 'TSLA', 'GC')
        
    Returns:
        Dictionary with trading hours information
    """
    # Stock trading hours (NYSE/NASDAQ)
    if product == 'TSLA':
        return {
            'exchange': 'NYSE',
            'start_time': '09:30',
            'end_time': '16:00',
            'timezone': 'US/Eastern',
            'description': 'Regular market hours (9:30 AM - 4:00 PM ET)',
            'is_24h': False
        }
    
    # Futures trading hours (CME)
    elif product in ['ES', 'NQ', 'YM', 'RTY']:
        return {
            'exchange': 'CME',
            'start_time': '18:00',  # Previous day
            'end_time': '17:00',    # Current day
            'timezone': 'US/Central',
            'description': 'Nearly 24-hour trading (6:00 PM CT - 5:00 PM CT)',
            'is_24h': True
        }
    
    # Commodity futures
    elif product in ['CL', 'GC', 'SI', 'NG']:
        return {
            'exchange': 'NYMEX',
            'start_time': '18:00',  # Previous day
            'end_time': '17:00',    # Current day
            'timezone': 'US/Central',
            'description': 'Nearly 24-hour trading (6:00 PM CT - 5:00 PM CT)',
            'is_24h': True
        }
    
    # Bond futures
    elif product in ['ZB', 'ZN', 'ZF', 'ZT']:
        return {
            'exchange': 'CBOT',
            'start_time': '18:00',  # Previous day
            'end_time': '17:00',    # Current day
            'timezone': 'US/Central',
            'description': 'Nearly 24-hour trading (6:00 PM CT - 5:00 PM CT)',
            'is_24h': True
        }
    
    # Forex (24-hour)
    elif product in ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCAD', 'NZDUSD', 'USDCHF']:
        return {
            'exchange': 'FOREX',
            'start_time': '00:00',
            'end_time': '23:59',
            'timezone': 'UTC',
            'description': '24-hour trading',
            'is_24h': True
        }
    
    # Default to CME hours
    else:
        return {
            'exchange': 'CME',
            'start_time': '18:00',
            'end_time': '17:00',
            'timezone': 'US/Central',
            'description': 'Nearly 24-hour trading (6:00 PM CT - 5:00 PM CT)',
            'is_24h': True
        }


def is_trading_time(dt: datetime, product: str) -> bool:
    """
    Check if the given datetime is within trading hours for the product.
    
    Args:
        dt: Datetime to check
        product: Product symbol
        
    Returns:
        True if within trading hours, False otherwise
    """
    trading_hours = get_product_trading_hours(product)
    
    # Convert to appropriate timezone
    if dt.tzinfo is None:
        dt = pytz.timezone(trading_hours['timezone']).localize(dt)
    else:
        dt = dt.astimezone(pytz.timezone(trading_hours['timezone']))
    
    # Check if it's a trading day first
    if not is_trading_day(dt.date(), trading_hours['exchange']):
        return False
    
    # For 24-hour products, always return True during trading days
    if trading_hours['is_24h']:
        return True
    
    # For regular hours products (like TSLA), check specific times
    current_time = dt.time()
    start_time = datetime.strptime(trading_hours['start_time'], '%H:%M').time()
    end_time = datetime.strptime(trading_hours['end_time'], '%H:%M').time()
    
    return start_time <= current_time <= end_time


def get_trading_session_info(product: str, date: date) -> dict:
    """
    Get trading session information for a product on a specific date.
    
    Args:
        product: Product symbol
        date: Date to check
        
    Returns:
        Dictionary with session information
    """
    trading_hours = get_product_trading_hours(product)
    
    # Check if it's a trading day
    is_trading = is_trading_day(date, trading_hours['exchange'])
    
    if not is_trading:
        return {
            'is_trading_day': False,
            'reason': 'Weekend or holiday',
            'next_trading_day': None
        }
    
    # Get next trading day if needed
    next_trading = None
    if not is_trading:
        try:
            next_trading = get_trading_days(
                pd.Timestamp(date) + pd.Timedelta(days=1),
                pd.Timestamp(date) + pd.Timedelta(days=7),
                trading_hours['exchange']
            )
            if len(next_trading) > 0:
                next_trading = next_trading[0].date()
        except Exception:
            pass
    
    return {
        'is_trading_day': is_trading,
        'trading_hours': trading_hours,
        'next_trading_day': next_trading,
        'reason': 'Regular trading day' if is_trading else 'Weekend or holiday'
    }


def is_crypto_trading_day(check_date: date | str | pd.Timestamp) -> bool:
    """
    Check if a given date is a trading day for cryptocurrencies.
    Cryptocurrencies trade 24/7, so every day is a trading day.
    
    Args:
        check_date: Date to check
        
    Returns:
        Always True (crypto trades 24/7)
    """
    return True


def get_crypto_trading_days(
    start_date: date | str | pd.Timestamp,
    end_date: date | str | pd.Timestamp
) -> pd.DatetimeIndex:
    """
    Get all trading days in a date range for cryptocurrencies.
    Cryptocurrencies trade 24/7, so this returns all days in the range.
    
    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        
    Returns:
        DatetimeIndex of all days (crypto trades 24/7)
    """
    if isinstance(start_date, str):
        start_date = pd.Timestamp(start_date)
    if isinstance(end_date, str):
        end_date = pd.Timestamp(end_date)
    
    # Return all days since crypto trades 24/7
    return pd.date_range(start=start_date, end=end_date, freq='D')


def get_previous_crypto_trading_day(
    current_date: date | str | pd.Timestamp
) -> date:
    """
    Get the previous trading day for cryptocurrencies.
    Since crypto trades 24/7, this is simply the previous day.
    
    Args:
        current_date: Reference date
        
    Returns:
        Previous day as date object
    """
    if isinstance(current_date, str):
        current_date = pd.Timestamp(current_date).date()
    elif isinstance(current_date, pd.Timestamp):
        current_date = current_date.date()
    
    # For crypto, previous trading day is just the previous day
    prev = pd.Timestamp(current_date) - pd.Timedelta(days=1)
    return prev.date()


def is_trading_time(
    check_datetime: datetime | str | pd.Timestamp,
    product: str
) -> bool:
    """
    Check if a given datetime is within trading hours for a product.
    
    Args:
        check_datetime: Datetime to check
        product: Product symbol (e.g., 'ES', 'BTCUSD')
        
    Returns:
        True if within trading hours, False otherwise
    """
    if isinstance(check_datetime, str):
        check_datetime = pd.Timestamp(check_datetime)
    elif isinstance(check_datetime, pd.Timestamp):
        check_datetime = check_datetime
    
    # Check if this is a cryptocurrency
    try:
        from .yfinance_loader import is_crypto_symbol
        if is_crypto_symbol(product):
            return True  # Crypto trades 24/7
    except ImportError:
        pass
    
    # For traditional products, assume trading during business hours
    # This is a simplified check - in practice you'd want more sophisticated logic
    weekday = check_datetime.weekday()
    hour = check_datetime.hour
    
    # Basic check: Monday-Friday, 9 AM - 4 PM ET
    if weekday < 5 and 9 <= hour < 16:
        return True
    
    return False
