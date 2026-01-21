#!/usr/bin/env python3
"""
TSLA Integration Setup Script

This script helps users set up TSLA integration with Almanac Futures.
It checks dependencies, validates API credentials, and tests the integration.
"""

import os
import sys
import subprocess
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if required dependencies are installed."""
    logger.info("Checking dependencies...")
    
    try:
        import alpaca
        logger.info("✓ alpaca-py is installed")
        return True
    except ImportError:
        logger.error("✗ alpaca-py is not installed")
        logger.info("Install with: pip install alpaca-py>=0.20.0")
        return False

def check_environment_variables():
    """Check if required environment variables are set."""
    logger.info("Checking environment variables...")
    
    api_key = os.getenv('ALPACA_API_KEY')
    secret_key = os.getenv('ALPACA_SECRET_KEY')
    base_url = os.getenv('ALPACA_BASE_URL', 'https://paper-api.alpaca.markets')
    
    if not api_key:
        logger.error("✗ ALPACA_API_KEY environment variable not set")
        return False
    
    if not secret_key:
        logger.error("✗ ALPACA_SECRET_KEY environment variable not set")
        return False
    
    logger.info(f"✓ API Key: {api_key[:8]}...")
    logger.info(f"✓ Secret Key: {secret_key[:8]}...")
    logger.info(f"✓ Base URL: {base_url}")
    
    return True

def test_alpaca_connection():
    """Test connection to Alpaca API."""
    logger.info("Testing Alpaca API connection...")
    
    try:
        from almanac.data_sources.alpaca_loader import get_alpaca_client
        
        client = get_alpaca_client()
        if client:
            logger.info("✓ Alpaca API connection successful")
            return True
        else:
            logger.error("✗ Failed to create Alpaca client")
            return False
            
    except Exception as e:
        logger.error(f"✗ Alpaca connection test failed: {e}")
        return False

def test_tsla_data_loading():
    """Test TSLA data loading functionality."""
    logger.info("Testing TSLA data loading...")
    
    try:
        from almanac.data_sources.alpaca_loader import load_tsla_minute_data
        
        # Test with recent trading day
        test_date = datetime.now() - timedelta(days=1)
        test_date_str = test_date.strftime('%Y-%m-%d')
        
        logger.info(f"Attempting to load TSLA data for {test_date_str}...")
        
        df = load_tsla_minute_data(test_date_str, test_date_str)
        
        if not df.empty:
            logger.info(f"✓ Successfully loaded {len(df)} TSLA records")
            logger.info(f"  Date range: {df['time'].min()} to {df['time'].max()}")
            logger.info(f"  Price range: ${df['low'].min():.2f} - ${df['high'].max():.2f}")
            return True
        else:
            logger.warning("⚠ No TSLA data returned (may be weekend/holiday)")
            return True
            
    except Exception as e:
        logger.error(f"✗ TSLA data loading test failed: {e}")
        return False

def test_ui_integration():
    """Test UI integration."""
    logger.info("Testing UI integration...")
    
    try:
        from almanac.pages.components.filters import create_product_dropdown
        
        dropdown = create_product_dropdown()
        options = dropdown.options
        
        tsla_options = [opt for opt in options if opt['value'] == 'TSLA']
        
        if tsla_options:
            logger.info("✓ TSLA found in product dropdown")
            logger.info(f"  Label: {tsla_options[0]['label']}")
            return True
        else:
            logger.error("✗ TSLA not found in product dropdown")
            return False
            
    except Exception as e:
        logger.error(f"✗ UI integration test failed: {e}")
        return False

def test_trading_hours():
    """Test trading hours functionality."""
    logger.info("Testing trading hours...")
    
    try:
        from almanac.data_sources.calendar import get_product_trading_hours, is_trading_time
        
        # Test trading hours
        hours = get_product_trading_hours('TSLA')
        logger.info(f"✓ TSLA trading hours: {hours['start_time']} - {hours['end_time']} {hours['timezone']}")
        
        # Test trading time check
        test_time = datetime(2024, 1, 15, 10, 30)  # Monday 10:30 AM
        is_trading = is_trading_time(test_time, 'TSLA')
        logger.info(f"✓ Trading time check: {test_time} is {'trading' if is_trading else 'non-trading'} time")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Trading hours test failed: {e}")
        return False

def print_setup_instructions():
    """Print setup instructions."""
    logger.info("\n" + "="*60)
    logger.info("TSLA INTEGRATION SETUP INSTRUCTIONS")
    logger.info("="*60)
    
    print("""
1. INSTALL DEPENDENCIES:
   pip install alpaca-py>=0.20.0

2. GET ALPACA API CREDENTIALS:
   - Sign up at https://alpaca.markets/
   - Go to your dashboard and get API credentials
   - Use paper trading for testing

3. SET ENVIRONMENT VARIABLES:
   
   Windows (PowerShell):
   $env:ALPACA_API_KEY="your_api_key_here"
   $env:ALPACA_SECRET_KEY="your_secret_key_here"
   $env:ALPACA_BASE_URL="https://paper-api.alpaca.markets"
   
   Windows (Command Prompt):
   set ALPACA_API_KEY=your_api_key_here
   set ALPACA_SECRET_KEY=your_secret_key_here
   set ALPACA_BASE_URL=https://paper-api.alpaca.markets
   
   Linux/Mac:
   export ALPACA_API_KEY="your_api_key_here"
   export ALPACA_SECRET_KEY="your_secret_key_here"
   export ALPACA_BASE_URL="https://paper-api.alpaca.markets"

4. RUN THIS SETUP SCRIPT:
   python setup_tsla_integration.py

5. START THE APPLICATION:
   python runalmanac.py
""")

def main():
    """Main setup function."""
    logger.info("TSLA Integration Setup Script")
    logger.info("="*40)
    
    # Check if we're in the right directory
    if not os.path.exists('almanac'):
        logger.error("Please run this script from the Almanac Futures root directory")
        return False
    
    tests = [
        ("Dependencies", check_dependencies),
        ("Environment Variables", check_environment_variables),
        ("Alpaca Connection", test_alpaca_connection),
        ("TSLA Data Loading", test_tsla_data_loading),
        ("UI Integration", test_ui_integration),
        ("Trading Hours", test_trading_hours),
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    logger.info("\n" + "="*40)
    logger.info("SETUP SUMMARY")
    logger.info("="*40)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 TSLA integration setup complete!")
        logger.info("You can now use TSLA in the Almanac Futures application.")
    else:
        logger.warning(f"⚠️  {total - passed} tests failed.")
        logger.info("Please check the errors above and fix any issues.")
        print_setup_instructions()
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
