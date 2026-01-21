"""
Check Database Status - See what products/data are already loaded

Checks which products exist in RawIntradayData and DailyData tables.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from almanac.data_sources.db_config import get_engine
from sqlalchemy import text
import pandas as pd

def check_database_status():
    """Check what products and data ranges exist in the database."""
    
    try:
        engine = get_engine()
        
        print("=" * 60)
        print("DATABASE STATUS CHECK")
        print("=" * 60)
        print()
        
        # Check RawIntradayData (minute data)
        print("📊 MINUTE DATA (RawIntradayData):")
        print("-" * 60)
        
        query = text("""
            SELECT 
                contract_id,
                interval,
                COUNT(*) as row_count,
                MIN([time]) as min_time,
                MAX([time]) as max_time,
                DATEDIFF(day, MIN([time]), MAX([time])) as days_span
            FROM dbo.RawIntradayData
            GROUP BY contract_id, interval
            ORDER BY contract_id, interval
        """)
        
        minute_df = pd.read_sql(query, engine)
        
        if minute_df.empty:
            print("  ⚠️  No minute data found in database")
        else:
            print(f"  ✅ Found {len(minute_df)} product/interval combinations:")
            print()
            for _, row in minute_df.iterrows():
                print(f"    • {row['contract_id']:6s} | {row['interval']:6s} | "
                      f"{row['row_count']:>10,} rows | "
                      f"{row['min_time']} to {row['max_time']} ({row['days_span']} days)")
        
        print()
        print("📅 DAILY DATA (DailyData):")
        print("-" * 60)
        
        query = text("""
            SELECT 
                contract_id,
                COUNT(*) as row_count,
                MIN([time]) as min_time,
                MAX([time]) as max_time,
                DATEDIFF(day, MIN([time]), MAX([time])) as days_span
            FROM dbo.DailyData
            GROUP BY contract_id
            ORDER BY contract_id
        """)
        
        daily_df = pd.read_sql(query, engine)
        
        if daily_df.empty:
            print("  ⚠️  No daily data found in database")
        else:
            print(f"  ✅ Found {len(daily_df)} products:")
            print()
            for _, row in daily_df.iterrows():
                print(f"    • {row['contract_id']:6s} | "
                      f"{row['row_count']:>10,} rows | "
                      f"{row['min_time']} to {row['max_time']} ({row['days_span']} days)")
        
        print()
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)
        
        # Get unique products from both tables
        all_products_minute = set(minute_df['contract_id'].unique()) if not minute_df.empty else set()
        all_products_daily = set(daily_df['contract_id'].unique()) if not daily_df.empty else set()
        all_products = all_products_minute | all_products_daily
        
        print(f"  Total products in database: {len(all_products)}")
        if all_products:
            print(f"  Products: {', '.join(sorted(all_products))}")
        
        print()
        return {
            'minute_products': all_products_minute,
            'daily_products': all_products_daily,
            'all_products': all_products,
            'minute_df': minute_df,
            'daily_df': daily_df
        }
        
    except Exception as e:
        print(f"  ❌ Error connecting to database: {e}")
        print()
        print("Make sure:")
        print("  1. SQL Server is running")
        print("  2. RESEARCH server is accessible")
        print("  3. HistoricalData database exists")
        print("  4. You have proper permissions")
        return None


if __name__ == '__main__':
    check_database_status()
