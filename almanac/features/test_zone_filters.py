"""
Tests for Zone-Based Percentage Change Filters

Run with: python -m pytest almanac/features/test_zone_filters.py -v
Or directly: python almanac/features/test_zone_filters.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import pytz
from zone_filters import (
    ZoneFilterSpec,
    parse_zone_spec,
    compute_zone_pct_change,
    apply_zone_filters,
    _ensure_timezone_aware,
    NY_TZ
)


def create_test_minute_data(dates_and_values):
    """
    Create test minute data.
    
    Args:
        dates_and_values: List of (date, time_str, open, close) tuples
        
    Returns:
        DataFrame with minute OHLCV data
    """
    records = []
    for date_val, time_str, open_val, close_val in dates_and_values:
        dt = NY_TZ.localize(datetime.combine(date_val, datetime.strptime(time_str, "%H:%M").time()))
        records.append({
            'time': dt,
            'open': open_val,
            'high': close_val,  # Simplified
            'low': open_val,
            'close': close_val,
            'volume': 1000
        })
    
    return pd.DataFrame(records)


def test_target_tolerance_logic():
    """Test that target ± tolerance range works correctly."""
    print("\n=== Test: Target/Tolerance Logic ===")
    
    spec = ZoneFilterSpec(
        name="test",
        enabled=True,
        target_pct=1.0,
        tolerance_pct=0.2,
        start_day_offset=0,
        start_hour=9,
        start_minute=30,
        end_day_offset=0,
        end_hour=16,
        end_minute=0
    )
    
    min_pct, max_pct = spec.get_range()
    assert min_pct == 0.8, f"Expected min 0.8, got {min_pct}"
    assert max_pct == 1.2, f"Expected max 1.2, got {max_pct}"
    
    print(f"[PASS] Range calculated correctly: [{min_pct}, {max_pct}]")
    
    # Test negative target
    spec2 = ZoneFilterSpec(
        name="test2",
        enabled=True,
        target_pct=-0.5,
        tolerance_pct=0.3,
        start_day_offset=0,
        start_hour=9,
        start_minute=30,
        end_day_offset=0,
        end_hour=16,
        end_minute=0
    )
    
    min_pct2, max_pct2 = spec2.get_range()
    assert min_pct2 == -0.8, f"Expected min -0.8, got {min_pct2}"
    assert max_pct2 == -0.2, f"Expected max -0.2, got {max_pct2}"
    
    print(f"[PASS] Negative target works: [{min_pct2}, {max_pct2}]")


def test_offset_logic():
    """Test day offset logic (-1, 0, 1)."""
    print("\n=== Test: Day Offset Logic ===")
    
    # Create data for Dec 15 and Dec 14
    from datetime import date as date_cls
    d1 = date_cls(2024, 12, 14)  # T-1
    d2 = date_cls(2024, 12, 15)  # T-0 (analysis date)
    
    data = create_test_minute_data([
        # Dec 14: Opens at 100, closes at 101 (1% gain)
        (d1, "09:30", 100.0, 100.0),
        (d1, "16:00", 100.0, 101.0),
        # Dec 15: Opens at 101, closes at 102
        (d2, "09:30", 101.0, 101.0),
        (d2, "16:00", 101.0, 102.0),
    ])
    
    # Filter for T-1 session (prev day)
    spec_prev = ZoneFilterSpec(
        name="prev_session",
        enabled=True,
        target_pct=1.0,
        tolerance_pct=0.1,
        start_day_offset=-1,  # T-1
        start_hour=9,
        start_minute=30,
        end_day_offset=-1,    # T-1
        end_hour=16,
        end_minute=0
    )
    
    pct_change, status = compute_zone_pct_change(data, d2, spec_prev)
    assert pct_change is not None, f"Failed: {status}"
    assert abs(pct_change - 1.0) < 0.01, f"Expected ~1%, got {pct_change}%"
    
    print(f"[PASS] T-1 offset works: {pct_change:.2f}% (status: {status})")
    
    # Filter for T-0 session (current day)
    spec_curr = ZoneFilterSpec(
        name="curr_session",
        enabled=True,
        target_pct=1.0,
        tolerance_pct=0.5,
        start_day_offset=0,   # T-0
        start_hour=9,
        start_minute=30,
        end_day_offset=0,     # T-0
        end_hour=16,
        end_minute=0
    )
    
    pct_change2, status2 = compute_zone_pct_change(data, d2, spec_curr)
    assert pct_change2 is not None, f"Failed: {status2}"
    # Dec 15: 101 -> 102 = 0.99% gain
    assert abs(pct_change2 - 0.99) < 0.02, f"Expected ~0.99%, got {pct_change2}%"
    
    print(f"[PASS] T-0 offset works: {pct_change2:.2f}% (status: {status2})")


def test_midnight_crossing():
    """Test zone crossing midnight (overnight session)."""
    print("\n=== Test: Midnight Crossing ===")
    
    from datetime import date as date_cls
    d1 = date_cls(2024, 12, 14)
    d2 = date_cls(2024, 12, 15)
    
    # Overnight session: 16:00 T-1 to 08:00 T-0
    data = create_test_minute_data([
        (d1, "16:00", 100.0, 100.0),  # Start
        (d1, "20:00", 100.0, 100.5),  # Overnight
        (d2, "04:00", 100.5, 101.0),  # Early morning
        (d2, "08:00", 101.0, 102.0),  # End
    ])
    
    spec = ZoneFilterSpec(
        name="overnight",
        enabled=True,
        target_pct=2.0,
        tolerance_pct=0.5,
        start_day_offset=-1,
        start_hour=16,
        start_minute=0,
        end_day_offset=0,
        end_hour=8,
        end_minute=0
    )
    
    pct_change, status = compute_zone_pct_change(data, d2, spec)
    assert pct_change is not None, f"Failed: {status}"
    # 100 -> 102 = 2% gain
    assert abs(pct_change - 2.0) < 0.01, f"Expected ~2%, got {pct_change}%"
    
    print(f"[PASS] Midnight crossing works: {pct_change:.2f}% (status: {status})")


def test_missing_bars():
    """Test handling of missing bars (holidays, gaps)."""
    print("\n=== Test: Missing Bars ===")
    
    from datetime import date as date_cls
    d1 = date_cls(2024, 12, 14)
    d2 = date_cls(2024, 12, 15)  # Analysis date
    
    # Only have bars for d1, not d2
    data = create_test_minute_data([
        (d1, "09:30", 100.0, 100.0),
        (d1, "16:00", 100.0, 101.0),
    ])
    
    spec = ZoneFilterSpec(
        name="missing_test",
        enabled=True,
        target_pct=1.0,
        tolerance_pct=0.5,
        start_day_offset=0,  # T-0 (Dec 15)
        start_hour=9,
        start_minute=30,
        end_day_offset=0,
        end_hour=16,
        end_minute=0
    )
    
    pct_change, status = compute_zone_pct_change(data, d2, spec)
    assert pct_change is None, "Should return None for missing bars"
    assert "no bars" in status.lower(), f"Expected 'no bars' in status, got: {status}"
    
    print(f"[PASS] Missing bars handled gracefully: {status}")


def test_full_integration():
    """Test full filtering pipeline with multiple days and filters."""
    print("\n=== Test: Full Integration ===")
    
    # Create 3 days of data
    from datetime import date as date_cls
    d1 = date_cls(2024, 12, 13)
    d2 = date_cls(2024, 12, 14)
    d3 = date_cls(2024, 12, 15)
    
    data = create_test_minute_data([
        # Day 1: Prev session +1%, overnight +0.5%
        (d1, "09:30", 100.0, 100.0),
        (d1, "16:00", 100.0, 101.0),  # +1%
        (d2, "08:00", 101.0, 101.5),  # +0.5% overnight
        (d2, "09:30", 101.5, 101.5),
        (d2, "16:00", 101.5, 102.5),  # +1%
        (d3, "08:00", 102.5, 103.5),  # +1% overnight
        (d3, "09:30", 103.5, 103.5),
        (d3, "16:00", 103.5, 104.5),  # +1%
    ])
    
    # Filter: prev session should be 1.0% ± 0.2%
    filter1 = ZoneFilterSpec(
        name="prev_ny",
        enabled=True,
        target_pct=1.0,
        tolerance_pct=0.2,
        start_day_offset=-1,
        start_hour=9,
        start_minute=30,
        end_day_offset=-1,
        end_hour=16,
        end_minute=0
    )
    
    # Filter: overnight should be 1.0% ± 0.6% (both days should pass)
    filter2 = ZoneFilterSpec(
        name="overnight",
        enabled=True,
        target_pct=1.0,
        tolerance_pct=0.6,
        start_day_offset=-1,
        start_hour=16,
        start_minute=0,
        end_day_offset=0,
        end_hour=8,
        end_minute=0
    )
    
    filtered_df, diagnostics = apply_zone_filters(data, [filter1, filter2])
    
    print(f"Total days: {diagnostics['total_days']}")
    print(f"Days remaining: {diagnostics['days_remaining']}")
    print(f"Days dropped: {diagnostics['days_dropped']}")
    
    for filter_name, results in diagnostics['filters_applied'].items():
        print(f"\nFilter: {filter_name}")
        print(f"  Passing: {results['days_passing']}")
        print(f"  Failing: {results['days_failing']}")
        if results['errors']:
            for date, reason in list(results['errors'].items())[:2]:
                print(f"    {date}: {reason}")
    
    # Should have dropped day 1 (can't compute prev session)
    assert diagnostics['days_remaining'] >= 1, "Should have at least 1 day remaining"
    assert diagnostics['days_dropped'] >= 1, "Should have dropped at least 1 day"
    
    print("\n[PASS] Full integration test passed")


def test_parse_zone_spec():
    """Test parsing from UI inputs."""
    print("\n=== Test: Parse Zone Spec ===")
    
    # Valid spec
    spec = parse_zone_spec(
        name="test",
        enabled=True,
        target_pct=1.0,
        tolerance_pct=0.2,
        start_day_offset=-1,
        start_hour=9,
        start_minute=30,
        end_day_offset=-1,
        end_hour=16,
        end_minute=0
    )
    
    assert spec is not None
    assert spec.name == "test"
    assert spec.target_pct == 1.0
    print("[PASS] Valid spec parsed correctly")
    
    # Disabled spec
    spec2 = parse_zone_spec(
        name="test2",
        enabled=False,
        target_pct=None,
        tolerance_pct=None,
        start_day_offset=None,
        start_hour=None,
        start_minute=None,
        end_day_offset=None,
        end_hour=None,
        end_minute=None
    )
    
    assert spec2 is None
    print("[PASS] Disabled spec returns None")
    
    # Missing required parameters
    try:
        parse_zone_spec(
            name="test3",
            enabled=True,
            target_pct=None,  # Missing!
            tolerance_pct=0.2,
            start_day_offset=-1,
            start_hour=9,
            start_minute=30,
            end_day_offset=-1,
            end_hour=16,
            end_minute=0
        )
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "missing" in str(e).lower()
        print(f"[PASS] Missing parameters caught: {e}")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("ZONE FILTER TESTS")
    print("=" * 60)
    
    try:
        test_target_tolerance_logic()
        test_offset_logic()
        test_midnight_crossing()
        test_missing_bars()
        test_full_integration()
        test_parse_zone_spec()
        
        print("\n" + "=" * 60)
        print("[SUCCESS] ALL TESTS PASSED")
        print("=" * 60)
        return True
    except AssertionError as e:
        print("\n" + "=" * 60)
        print(f"[FAILED] TEST FAILED: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return False
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"[ERROR] TEST ERROR: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)

