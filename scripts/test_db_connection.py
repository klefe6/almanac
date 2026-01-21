"""
Test Database Connection - Try multiple connection methods

Tests different connection strings to find a working SQL Server connection.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import OperationalError
except ImportError:
    print("❌ SQLAlchemy not installed. Install with: pip install sqlalchemy pyodbc")
    sys.exit(1)


def test_connection(conn_string: str, description: str) -> bool:
    """Test a connection string and return True if successful."""
    try:
        engine = create_engine(conn_string, connect_args={'timeout': 5})
        with engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION"))
            version = result.fetchone()[0]
            print(f"  ✅ {description}")
            print(f"     Connection successful!")
            print(f"     SQL Server: {version[:80]}...")
            return True
    except OperationalError as e:
        print(f"  ❌ {description}")
        print(f"     Error: {str(e)[:100]}...")
        return False
    except Exception as e:
        print(f"  ❌ {description}")
        print(f"     Error: {type(e).__name__}: {str(e)[:100]}...")
        return False


def main():
    """Test various connection strings."""
    print("=" * 70)
    print("SQL SERVER CONNECTION TESTER")
    print("=" * 70)
    print()
    
    # Test connections
    connections = [
        # Local SQL Server instances
        (
            "mssql+pyodbc://@localhost/HistoricalData?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
            "Localhost (default instance)"
        ),
        (
            "mssql+pyodbc://@localhost,1433/HistoricalData?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
            "Localhost (port 1433)"
        ),
        (
            "mssql+pyodbc://@localhost\\SQLEXPRESS/HistoricalData?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
            "Localhost (SQLEXPRESS instance)"
        ),
        (
            "mssql+pyodbc://@localhost\\MSSQLSERVER/HistoricalData?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
            "Localhost (MSSQLSERVER instance)"
        ),
        (
            "mssql+pyodbc://@./HistoricalData?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
            "Local (dot notation)"
        ),
        (
            "mssql+pyodbc://@(local)/HistoricalData?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
            "Local (local keyword)"
        ),
        # Remote RESEARCH server (original)
        (
            "mssql+pyodbc://@RESEARCH/HistoricalData?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
            "RESEARCH server (original)"
        ),
        (
            "mssql+pyodbc://@RESEARCH\\SQLEXPRESS/HistoricalData?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
            "RESEARCH server (SQLEXPRESS)"
        ),
        (
            "mssql+pyodbc://@RESEARCH,1433/HistoricalData?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
            "RESEARCH server (port 1433)"
        ),
    ]
    
    print("Testing connection strings...")
    print()
    
    working_connections = []
    
    for conn_string, description in connections:
        if test_connection(conn_string, description):
            working_connections.append((conn_string, description))
        print()
    
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    
    if working_connections:
        print(f"\n✅ Found {len(working_connections)} working connection(s):\n")
        for conn_string, description in working_connections:
            print(f"  {description}")
            print(f"  Connection string: {conn_string}")
            print()
        
        print("To use this connection, update db_config.py:")
        print(f'  DEFAULT_DB_CONN_STRING = "{working_connections[0][0]}"')
    else:
        print("\n❌ No working connections found.\n")
        print("Possible solutions:")
        print("  1. Install SQL Server locally (SQL Server Express is free)")
        print("  2. Verify RESEARCH server is accessible on your network")
        print("  3. Check if SQL Server Browser service is running")
        print("  4. Verify Windows authentication is enabled")
        print("  5. Check firewall settings")
        print("\nFor local development, consider:")
        print("  - SQL Server Express (free, lightweight)")
        print("  - Or use file-based loading (already working)")


if __name__ == '__main__':
    main()

