"""
Input Validation Utilities

Provides validation functions for user inputs in the Almanac Futures application.
"""

import pandas as pd
from datetime import datetime, date
from typing import Union, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


def validate_product(product: str) -> bool:
    """
    Validate product symbol.
    
    Args:
        product (str): Product symbol to validate
        
    Returns:
        bool: True if valid
        
    Raises:
        ValidationError: If product is invalid
    """
    if not product or not isinstance(product, str):
        raise ValidationError("Product must be a non-empty string")
    
    valid_products = [
        'ES', 'NQ', 'YM', 'RTY', 'CL', 'GC', 'SI', 'NG',
        'ZB', 'ZN', 'ZF', 'ZT', 'EURUSD', 'GBPUSD', 'USDJPY',
        'AUDUSD', 'USDCAD', 'NZDUSD', 'USDCHF', 'TSLA', 'BTCUSD'
    ]
    
    if product.upper() not in valid_products:
        raise ValidationError(f"Invalid product: {product}. Must be one of {valid_products}")
    
    return True


def validate_date_range(start_date: Union[str, date], end_date: Union[str, date]) -> Tuple[date, date]:
    """
    Validate and normalize date range.
    
    Args:
        start_date: Start date (string or date object)
        end_date: End date (string or date object)
        
    Returns:
        Tuple[date, date]: Normalized start and end dates
        
    Raises:
        ValidationError: If dates are invalid
    """
    try:
        # Convert strings to date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Validate date objects
        if not isinstance(start_date, date) or not isinstance(end_date, date):
            raise ValidationError("Dates must be valid date objects or strings in YYYY-MM-DD format")
        
        # Check date range validity
        if start_date > end_date:
            raise ValidationError("Start date must be before or equal to end date")
        
        # Check reasonable date range (not too far in the past or future)
        min_date = date(2000, 1, 1)
        max_date = date(2030, 12, 31)
        
        if start_date < min_date or end_date > max_date:
            raise ValidationError(f"Date range must be between {min_date} and {max_date}")
        
        return start_date, end_date
        
    except ValueError as e:
        raise ValidationError(f"Invalid date format: {e}")


def validate_threshold(value: Union[int, float], name: str, min_val: float = 0, max_val: float = 1000) -> float:
    """
    Validate threshold values.
    
    Args:
        value: Threshold value to validate
        name: Name of the threshold for error messages
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        
    Returns:
        float: Validated threshold value
        
    Raises:
        ValidationError: If threshold is invalid
    """
    if value is None:
        raise ValidationError(f"{name} cannot be None")
    
    try:
        float_val = float(value)
    except (ValueError, TypeError):
        raise ValidationError(f"{name} must be a number")
    
    if float_val < min_val or float_val > max_val:
        raise ValidationError(f"{name} must be between {min_val} and {max_val}")
    
    return float_val


def validate_filters(filters: Optional[List[str]]) -> List[str]:
    """
    Validate filter list.
    
    Args:
        filters: List of filter names
        
    Returns:
        List[str]: Validated filter list
        
    Raises:
        ValidationError: If filters are invalid
    """
    if filters is None:
        return []
    
    if not isinstance(filters, list):
        raise ValidationError("Filters must be a list")
    
    valid_filters = [
        'high_volume', 'low_volume', 'gap_up', 'gap_down',
        'timeA_gt_timeB', 'timeA_lt_timeB'
    ]
    
    for filter_name in filters:
        if not isinstance(filter_name, str):
            raise ValidationError("All filters must be strings")
        if filter_name not in valid_filters:
            raise ValidationError(f"Invalid filter: {filter_name}. Must be one of {valid_filters}")
    
    return filters


def validate_dataframe(df: pd.DataFrame, name: str, required_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Validate DataFrame structure and content.
    
    Args:
        df: DataFrame to validate
        name: Name of the DataFrame for error messages
        required_columns: List of required column names
        
    Returns:
        pd.DataFrame: Validated DataFrame
        
    Raises:
        ValidationError: If DataFrame is invalid
    """
    if not isinstance(df, pd.DataFrame):
        raise ValidationError(f"{name} must be a pandas DataFrame")
    
    if df.empty:
        raise ValidationError(f"{name} cannot be empty")
    
    if required_columns:
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValidationError(f"{name} missing required columns: {missing_columns}")
    
    return df


def validate_callback_inputs(prod: str, start: str, end: str, filters: List[str], 
                           vol_thr: float, pct_thr: float) -> Tuple[str, date, date, List[str], float, float]:
    """
    Validate all callback inputs at once.
    
    Args:
        prod: Product symbol
        start: Start date string
        end: End date string
        filters: List of filters
        vol_thr: Volume threshold
        pct_thr: Percentage threshold
        
    Returns:
        Tuple: Validated inputs
        
    Raises:
        ValidationError: If any input is invalid
    """
    try:
        # Validate product
        validate_product(prod)
        
        # Validate dates
        start_date, end_date = validate_date_range(start, end)
        
        # Validate filters
        validated_filters = validate_filters(filters)
        
        # Validate thresholds
        validated_vol_thr = validate_threshold(vol_thr, "Volume threshold", 0, 1000)
        validated_pct_thr = validate_threshold(pct_thr, "Percentage threshold", 0, 10)
        
        logger.info(f"Input validation successful: {prod}, {start_date} to {end_date}, "
                   f"{len(validated_filters)} filters, vol_thr={validated_vol_thr}, pct_thr={validated_pct_thr}")
        
        return prod, start_date, end_date, validated_filters, validated_vol_thr, validated_pct_thr
        
    except ValidationError as e:
        logger.error(f"Input validation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected validation error: {e}")
        raise ValidationError(f"Validation error: {e}")


def safe_convert_to_numeric(value, default=0, name="value"):
    """
    Safely convert a value to numeric, returning default if conversion fails.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        name: Name of the value for logging
        
    Returns:
        float: Converted numeric value
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert {name} to numeric, using default: {default}")
        return default


def safe_get_list_length(lst, name="list"):
    """
    Safely get the length of a list, handling None values.
    
    Args:
        lst: List to get length of
        name: Name of the list for logging
        
    Returns:
        int: Length of the list (0 if None)
    """
    if lst is None:
        logger.debug(f"{name} is None, returning length 0")
        return 0
    return len(lst)
