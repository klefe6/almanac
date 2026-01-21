"""
Almanac Futures - Application Launcher

Simple entry point to run the Dash application.

Usage:
    python runalmanac.py
    
    # Or with custom settings:
    python runalmanac.py --port 8087 --debug
"""

import argparse
import logging
import os
import sys
from datetime import datetime

# Early error handling for imports
try:
    print("Importing almanac modules...", file=sys.stderr)
    from almanac.config import get_config
    print("✓ Config imported", file=sys.stderr)
    from almanac.app import run_server
    print("✓ App imported", file=sys.stderr)
except ImportError as e:
    print(f"❌ Import error: {e}", file=sys.stderr)
    print(f"Python path: {sys.executable}", file=sys.stderr)
    print(f"Python version: {sys.version}", file=sys.stderr)
    raise
except Exception as e:
    print(f"❌ Error during import: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc()
    raise


def setup_logging():
    """
    Set up logging configuration with both console and file output.
    Creates logs directory if it doesn't exist.
    """
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(logs_dir, f'almanac_{timestamp}.log')
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()  # Console output
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_filename}")
    return logger


def main():
    # Set up logging first
    print("Setting up logging...", file=sys.stderr)
    logger = setup_logging()
    
    print("Getting configuration...", file=sys.stderr)
    cfg = get_config()
    
    parser = argparse.ArgumentParser(description='Almanac Futures Application')
    parser.add_argument('--host', default=cfg.host, help=f'Host address (default: {cfg.host})')
    parser.add_argument('--port', type=int, default=cfg.port, help=f'Port number (default: {cfg.port})')
    parser.add_argument('--no-debug', dest='debug', action='store_false', help='Disable debug mode')
    parser.set_defaults(debug=cfg.debug)
    
    args = parser.parse_args()
    
    # Log startup information
    logger.info(f"Starting Almanac Futures on http://{args.host}:{args.port}")
    logger.info(f"Debug mode: {'ON' if args.debug else 'OFF'}")
    
    # Keep console output for user interaction
    print(f"Starting Almanac Futures on http://{args.host}:{args.port}")
    print(f"Debug mode: {'ON' if args.debug else 'OFF'}")
    print("\nPress Ctrl+C to stop the server.")
    
    try:
        print("Starting server...", file=sys.stderr)
        run_server(host=args.host, port=args.port, debug=args.debug)
    except KeyboardInterrupt:
        logger.info("Server stopped by user (Ctrl+C)")
        print("\nServer stopped.")
    except Exception as e:
        logger.error(f"Server error: {str(e)}", exc_info=True)
        print(f"\n❌ Server error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()

