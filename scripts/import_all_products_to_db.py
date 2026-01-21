"""
Purpose: Import all available products from local files into SQL Server.
Author: Kevin Lefebvre
Last Updated: 2025-11-05

Loads all products found under 1min/ and daily/ into dbo.RawIntradayData and
dbo.DailyData respectively. Skips a product if any rows for that product are
already present in the target table. Creates tables and indexes if missing.

Usage:
  python scripts/import_all_products_to_db.py

Notes:
- Uses the configured SQL Server connection via get_engine().
- Continues on errors; prints a final summary of skips/inserts/errors.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

# Ensure package imports work when running as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from almanac.data_sources.db_config import get_engine
from almanac.data_sources.file_loader import (
    load_minute_data_from_file,
    load_daily_data_from_file,
)


DATA_DIR = ROOT


def _test_connection() -> bool:
    """Test database connection and return True if successful."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print()
        print("Troubleshooting steps:")
        print("  1. Verify SQL Server is running on RESEARCH server")
        print("  2. Check if you can connect via SQL Server Management Studio")
        print("  3. Verify network connectivity to RESEARCH server")
        print("  4. Check if SQL Server allows remote connections")
        print("  5. Try alternative connection string in db_config.py:")
        print("     - Option 2: Named instance (SQLEXPRESS)")
        print("     - Option 3: Explicit port (1433)")
        print("     - Option 4: IP address instead of hostname")
        print()
        return False


def _ensure_tables_and_indexes() -> None:
    """Create tables and indexes if they don't already exist."""
    engine = get_engine()

    ddl = text(
        """
        -- Create RawIntradayData if missing
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.name = 'RawIntradayData' AND s.name = 'dbo'
        )
        BEGIN
            CREATE TABLE dbo.RawIntradayData (
                contract_id    VARCHAR(16)    NOT NULL,
                interval       VARCHAR(16)    NOT NULL,
                [time]         DATETIME2      NOT NULL,
                [open]         FLOAT          NOT NULL,
                [high]         FLOAT          NOT NULL,
                [low]          FLOAT          NOT NULL,
                [close]        FLOAT          NOT NULL,
                [volume]       BIGINT         NOT NULL,
                CONSTRAINT PK_RawIntraday PRIMARY KEY CLUSTERED (contract_id, interval, [time])
            );
        END;

        -- Create DailyData if missing
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.name = 'DailyData' AND s.name = 'dbo'
        )
        BEGIN
            CREATE TABLE dbo.DailyData (
                contract_id    VARCHAR(16)    NOT NULL,
                [time]         DATETIME2      NOT NULL,
                [open]         FLOAT          NOT NULL,
                [high]         FLOAT          NOT NULL,
                [low]          FLOAT          NOT NULL,
                [close]        FLOAT          NOT NULL,
                [volume]       BIGINT         NOT NULL,
                CONSTRAINT PK_Daily PRIMARY KEY CLUSTERED (contract_id, [time])
            );
        END;

        -- Create helpful indexes if missing
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_minute_lookup')
        BEGIN
            CREATE NONCLUSTERED INDEX idx_minute_lookup
            ON dbo.RawIntradayData (contract_id, interval, [time])
            INCLUDE ([open], [high], [low], [close], [volume]);
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_daily_lookup')
        BEGIN
            CREATE NONCLUSTERED INDEX idx_daily_lookup
            ON dbo.DailyData (contract_id, [time])
            INCLUDE ([open], [high], [low], [close], [volume]);
        END;
        """
    )

    with engine.begin() as conn:
        conn.execute(ddl)


def _get_products_from_files() -> List[str]:
    """Return unique product symbols inferred from 1min/*.txt and daily/*_daily.txt."""
    one_min_dir = DATA_DIR / "1min"
    daily_dir = DATA_DIR / "daily"

    products: Set[str] = set()

    if one_min_dir.exists():
        for p in one_min_dir.glob("*.txt"):
            products.add(p.stem.upper())

    if daily_dir.exists():
        for p in daily_dir.glob("*_daily.txt"):
            products.add(p.stem.replace("_daily", "").upper())

    return sorted(products)


def _product_has_minute_data(product: str) -> bool:
    engine = get_engine()
    sql = text(
        """
        SELECT TOP 1 1
        FROM dbo.RawIntradayData
        WHERE contract_id = :prod AND interval = '1min'
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"prod": product}).fetchone()
    return row is not None


def _product_has_daily_data(product: str) -> bool:
    engine = get_engine()
    sql = text(
        """
        SELECT TOP 1 1
        FROM dbo.DailyData
        WHERE contract_id = :prod
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"prod": product}).fetchone()
    return row is not None


def _insert_minute(product: str) -> Tuple[str, str, Optional[str]]:
    """Insert full minute data for a product. Returns (product, status, error)."""
    try:
        if _product_has_minute_data(product):
            return product, "skipped_minute", None

        # Load all rows from file
        df = load_minute_data_from_file(product, "1900-01-01", "2100-01-01", validate=False)
        if df.empty:
            return product, "no_data_minute", None

        df = df.copy()
        df["contract_id"] = product
        df["interval"] = "1min"

        # Reorder columns to match table
        df = df[[
            "contract_id",
            "interval",
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]]

        engine = get_engine()
        # Chunked append for large datasets
        df.to_sql(
            "RawIntradayData",
            engine,
            schema="dbo",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=50000,
        )
        return product, "inserted_minute", None
    except Exception as e:
        return product, "error_minute", str(e)


def _insert_daily(product: str) -> Tuple[str, str, Optional[str]]:
    """Insert full daily data for a product. Returns (product, status, error)."""
    try:
        if _product_has_daily_data(product):
            return product, "skipped_daily", None

        df = load_daily_data_from_file(product, "1900-01-01", "2100-01-01", add_derived_fields=False)
        if df.empty:
            return product, "no_data_daily", None

        # Ensure required columns
        # file_loader returns columns: ['date','time','open','high','low','close','volume']
        df = df.copy()
        df["contract_id"] = product
        # Use 'time' as-is (datetime)

        # Reorder columns to match table
        df = df[[
            "contract_id",
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]]

        engine = get_engine()
        df.to_sql(
            "DailyData",
            engine,
            schema="dbo",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10000,
        )
        return product, "inserted_daily", None
    except Exception as e:
        return product, "error_daily", str(e)


def main() -> None:
    """Import all products, skipping those already in the database."""
    print("=" * 60)
    print("DATABASE IMPORT - All Products")
    print("=" * 60)
    print()
    
    # Test connection first
    print("Testing database connection...")
    if not _test_connection():
        print()
        print("⚠️  Cannot proceed without database connection.")
        return
    
    print("✅ Database connection successful")
    print()
    
    # Ensure tables exist
    print("Ensuring tables and indexes exist...")
    try:
        _ensure_tables_and_indexes()
        print("✅ Tables and indexes ready")
    except Exception as e:
        print(f"❌ Failed to ensure tables/indexes: {e}")
        return
    
    print()

    products = _get_products_from_files()
    if not products:
        print("⚠️  No products found under 1min/ or daily/. Nothing to import.")
        return

    print(f"Found {len(products)} products to consider: {', '.join(products)}")
    print()

    results: Dict[str, Dict[str, Optional[str]]] = {}

    for prod in products:
        minute_status_prod, minute_status, minute_err = _insert_minute(prod)
        daily_status_prod, daily_status, daily_err = _insert_daily(prod)

        results[prod] = {
            "minute": minute_status,
            "minute_error": minute_err,
            "daily": daily_status,
            "daily_error": daily_err,
        }

        line = f"• {prod:6s} | minute={minute_status:15s} | daily={daily_status:15s}"
        if minute_err:
            line += f" | minute_err={minute_err}"
        if daily_err:
            line += f" | daily_err={daily_err}"
        print(line)

    # Summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)

    counts = {
        "inserted_minute": 0,
        "inserted_daily": 0,
        "skipped_minute": 0,
        "skipped_daily": 0,
        "no_data_minute": 0,
        "no_data_daily": 0,
        "error_minute": 0,
        "error_daily": 0,
    }

    failed: List[str] = []

    for prod, res in results.items():
        counts[res["minute"]] = counts.get(res["minute"], 0) + 1
        counts[res["daily"]] = counts.get(res["daily"], 0) + 1
        if res["minute"].startswith("error") or res["daily"].startswith("error"):
            failed.append(prod)

    for k in [
        "inserted_minute",
        "inserted_daily",
        "skipped_minute",
        "skipped_daily",
        "no_data_minute",
        "no_data_daily",
        "error_minute",
        "error_daily",
    ]:
        print(f"{k:16s}: {counts.get(k, 0)}")

    if failed:
        print()
        print(f"Products with errors ({len(failed)}): {', '.join(sorted(failed))}")


if __name__ == "__main__":
    main()


