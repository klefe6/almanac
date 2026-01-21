# Almanac Futures - Performance Optimization Opportunities

**Analysis Date**: 2025-01-22  
**Sorted by Impact: Biggest → Smallest**

---

## 🔴 **TIER 1: CRITICAL - Biggest Impact (10-100x improvement potential)**

### 1. **Replace `.apply()` with Vectorized Operations in Filters** ⚡
**Impact**: 50-100x faster  
**Location**: `almanac/features/filters.py`

**Current Issues**:
- Line 142: `df['prev_date'] = df['date'].apply(lambda d: get_previous_trading_day(d))` - **SLOW**
- Line 188: `df[df['date'].apply(lambda d: is_economic_event_date(d, event_type))]` - **VERY SLOW**
- Line 200: `df['week_start'] = df_dt.apply(lambda d: get_week_start(d))` - **SLOW**
- Line 215: Using `.apply()` for date string formatting

**Optimization**:
```python
# Instead of: df['prev_date'] = df['date'].apply(lambda d: get_previous_trading_day(d))
# Use vectorized operations or pre-computed mapping
date_mapping = {date: get_previous_trading_day(date) for date in df['date'].unique()}
df['prev_date'] = df['date'].map(date_mapping)
```
**Estimated Speedup**: 50-100x for large datasets (100k+ rows)

---

### 2. **Optimize Statistics Computation - Single Pass GroupBy** ⚡
**Impact**: 3-5x faster  
**Location**: `almanac/features/stats.py`

**Current Issues**:
- Lines 62-67: Multiple `.apply()` calls per group
- Line 76: Duplicate computation for range stats
- Line 54: `x.mode().iloc[0]` computed for every group - **VERY EXPENSIVE**

**Optimization**:
```python
# Compute all statistics in ONE pass using agg()
stats = grp['pct_chg'].agg([
    ('mean', 'mean'),
    ('median', 'median'),
    ('trimmed_mean', lambda x: trimmed_mean(x, trim_pct)),
    ('var', 'var')
])
# Pre-compute mode using scipy.stats.mode() or optimize
```
**Estimated Speedup**: 3-5x for statistics computation

---

### 3. **Implement Proper Database Indexing** 📊
**Impact**: 10-50x faster queries  
**Location**: Database schema (not in codebase yet)

**Current Issues**:
- No database indexes visible in queries
- Full table scans for date range queries
- No composite indexes on (contract_id, time, interval)

**Optimization**:
```sql
CREATE INDEX idx_contract_time ON RawIntradayData(contract_id, time, interval);
CREATE INDEX idx_daily_contract_time ON DailyData(contract_id, time);
```
**Estimated Speedup**: 10-50x for database queries (depends on data size)

---

### 4. **Parallel Data Loading (Daily + Minute Simultaneously)** 🚀
**Impact**: 2x faster initial load  
**Location**: `almanac/pages/profile.py` lines 1651-1652

**Current Issues**:
- Sequential loading: `daily = load_daily_data()` then `minute = load_minute_data()`
- Both are independent operations that can run in parallel

**Optimization**:
```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=2) as executor:
    daily_future = executor.submit(load_daily_data, prod, start, end)
    minute_future = executor.submit(load_minute_data, prod, start, end)
    daily = daily_future.result()
    minute = minute_future.result()
```
**Estimated Speedup**: 2x for data loading phase

---

## 🟡 **TIER 2: HIGH IMPACT (3-10x improvement potential)**

### 5. **Cache Economic Event Lookups** 💾
**Impact**: 5-10x faster for event filters  
**Location**: `almanac/features/filters.py` line 188

**Current Issues**:
- `is_economic_event_date()` called for every row
- Date string formatting on every call
- No caching of event date sets

**Optimization**:
```python
# Pre-compute event date sets once
_fomc_dates_cache = None
def get_fomc_dates_set():
    global _fomc_dates_cache
    if _fomc_dates_cache is None:
        from ..data_sources.economic_events import get_economic_event_dates
        _fomc_dates_cache = {pd.to_datetime(d).date() for d in get_economic_event_dates('FOMC')}
    return _fomc_dates_cache

# Then use: df[df['date'].isin(get_fomc_dates_set())]
```
**Estimated Speedup**: 5-10x for event-based filters

---

### 6. **Optimize DataFrame Copies - Use Views Where Possible** 📋
**Impact**: 2-3x memory reduction, 1.5-2x speedup  
**Location**: Throughout filtering code

**Current Issues**:
- Line 138: `df = minute_df.copy()` - full copy
- Multiple intermediate copies during filtering
- Unnecessary copies when filtering can be done in-place

**Optimization**:
```python
# Use copy only when necessary (e.g., when modifying in-place)
df = minute_df.copy()  # Only if we'll modify
# For read-only operations, use views or boolean indexing directly
```
**Estimated Speedup**: 1.5-2x, plus 2-3x memory reduction

---

### 7. **Pre-compute Week Starts Once** 📅
**Impact**: 3-5x faster for FOMC week filter  
**Location**: `almanac/features/filters.py` line 200

**Current Issues**:
- Computing `week_start` for every row using `.apply()`
- Converting dates multiple times

**Optimization**:
```python
# Vectorized week start calculation
df_dt = pd.to_datetime(df['date'])
df['week_start'] = (df_dt - pd.to_timedelta(df_dt.dt.dayofweek, unit='D')).dt.date
```
**Estimated Speedup**: 3-5x for week-based filters

---

### 8. **Implement Redis Caching (Currently using in-memory)** 🔴
**Impact**: 5-10x faster for repeated queries, shared across sessions  
**Location**: Cache configuration

**Current Issues**:
- Using in-memory cache (Dash's default)
- Cache not shared across server restarts
- Cache timeout too short (300 seconds)

**Optimization**:
```python
import redis
from dash import DiskcacheManager
import diskcache as dc

cache = dc.Cache("./cache")
# Or use Redis:
cache = redis.Redis(host='localhost', port=6379, db=0)
```
**Estimated Speedup**: 5-10x for repeated operations, persistent cache

---

## 🟢 **TIER 3: MEDIUM IMPACT (1.5-3x improvement potential)**

### 9. **Lazy Load HOD/LOD Analysis** 🔄
**Impact**: 2-3x faster initial response  
**Location**: `almanac/pages/profile.py` lines 1735-1772

**Current Issues**:
- HOD/LOD computed on every calculation
- Computed even if user doesn't need it
- Blocks main callback completion

**Optimization**:
- Move HOD/LOD to separate callback triggered by button
- Use background task or dcc.Interval for async computation
- Return placeholder, update when ready
**Estimated Speedup**: 2-3x perceived speed for initial response

---

### 10. **Optimize Mode Calculation in Stats** 📊
**Impact**: 2-3x faster statistics  
**Location**: `almanac/features/stats.py` lines 54, 140

**Current Issues**:
- `x.mode().iloc[0]` is expensive for every group
- Mode calculation requires sorting/histogram

**Optimization**:
```python
# Use scipy.stats.mode() or optimize mode calculation
from scipy import stats
mode_val = stats.mode(x, keepdims=True)[0][0] if len(x) > 0 else x.median()
# Or skip mode if not needed, use approximate mode
```
**Estimated Speedup**: 2-3x for mode calculation

---

### 11. **Batch Database Queries** 🗄️
**Impact**: 1.5-2x faster for multiple products  
**Location**: Database loading functions

**Current Issues**:
- One query per product
- No batching for multiple date ranges

**Optimization**:
```python
# If loading multiple products, batch queries
SELECT * FROM RawIntradayData 
WHERE contract_id IN (:products) AND time BETWEEN :start AND :end
```
**Estimated Speedup**: 1.5-2x when loading multiple products

---

### 12. **Use pd.read_parquet() for File-Based Data** 📁
**Impact**: 2-3x faster file loading  
**Location**: `almanac/data_sources/file_loader.py`

**Current Issues**:
- Using text file parsing
- Slow for large files

**Optimization**:
- Convert to Parquet format
- Use `pd.read_parquet()` which is 10x+ faster
**Estimated Speedup**: 2-3x for file-based loading

---

## 🔵 **TIER 4: LOWER IMPACT (1.2-1.5x improvement potential)**

### 13. **Optimize String Operations** 📝
**Impact**: 1.2-1.5x faster for date formatting  
**Location**: Various date string conversions

**Current Issues**:
- Multiple `.strftime()` calls
- Date string comparisons

**Optimization**:
- Pre-compute date strings once
- Use date objects instead of strings where possible
**Estimated Speedup**: 1.2-1.5x

---

### 14. **Remove Redundant Calculations** ♻️
**Impact**: 1.2-1.5x faster  
**Location**: Throughout codebase

**Current Issues**:
- `df['date'] = df['time'].dt.date` computed multiple times
- Duplicate datetime conversions

**Optimization**:
- Cache computed columns
- Reuse date columns instead of recomputing
**Estimated Speedup**: 1.2-1.5x

---

### 15. **Optimize Chart Generation** 📊
**Impact**: 1.2-1.5x faster rendering  
**Location**: Chart generation functions

**Current Issues**:
- Generating all charts even if not visible
- Large data series sent to frontend

**Optimization**:
- Lazy load charts
- Downsample data for display (>1000 points)
- Use plotly's built-in downsampling
**Estimated Speedup**: 1.2-1.5x for chart rendering

---

### 16. **Implement Query Result Streaming** 🌊
**Impact**: 1.2-1.5x faster for large datasets  
**Location**: Database queries

**Current Issues**:
- Loading entire result set into memory
- Blocking on full data load

**Optimization**:
- Stream results in chunks
- Process as data arrives
**Estimated Speedup**: 1.2-1.5x for very large datasets

---

### 17. **Reduce Debug Print Statements** 🔇
**Impact**: 1.1-1.2x faster (small but consistent)  
**Location**: Throughout codebase

**Current Issues**:
- Many `print()` statements in hot paths
- String formatting overhead

**Optimization**:
- Use logging with appropriate levels
- Disable in production
- Use conditional compilation
**Estimated Speedup**: 1.1-1.2x (minimal but consistent)

---

## 📊 **Summary by Estimated Total Impact**

| Tier | Optimization | Estimated Speedup | Effort | Priority |
|------|-------------|------------------|--------|----------|
| 🔴 1 | Vectorize filter operations | 50-100x | Medium | ⭐⭐⭐⭐⭐ |
| 🔴 1 | Database indexing | 10-50x | Low | ⭐⭐⭐⭐⭐ |
| 🔴 1 | Single-pass statistics | 3-5x | Medium | ⭐⭐⭐⭐⭐ |
| 🔴 1 | Parallel data loading | 2x | Low | ⭐⭐⭐⭐ |
| 🟡 2 | Cache event lookups | 5-10x | Low | ⭐⭐⭐⭐ |
| 🟡 2 | Reduce DataFrame copies | 1.5-2x | Low | ⭐⭐⭐ |
| 🟡 2 | Vectorize week calculations | 3-5x | Low | ⭐⭐⭐ |
| 🟡 2 | Redis caching | 5-10x | Medium | ⭐⭐⭐ |
| 🟢 3 | Lazy load HOD/LOD | 2-3x | Medium | ⭐⭐ |
| 🟢 3 | Optimize mode calc | 2-3x | Low | ⭐⭐ |
| 🟢 3 | Batch DB queries | 1.5-2x | Medium | ⭐⭐ |

---

## 🎯 **Recommended Implementation Order**

1. **Week 1 (Biggest Impact)**:
   - Vectorize filter operations (#1)
   - Add database indexes (#3)
   - Cache event lookups (#5)

2. **Week 2 (High Impact)**:
   - Optimize statistics computation (#2)
   - Implement parallel loading (#4)
   - Reduce DataFrame copies (#6)

3. **Week 3 (Medium Impact)**:
   - Redis caching (#8)
   - Lazy load HOD/LOD (#9)
   - Optimize mode calculation (#10)

4. **Week 4 (Polish)**:
   - Remaining Tier 3-4 optimizations
   - Performance monitoring
   - Benchmarking

---

## 📈 **Expected Overall Performance Gain**

**Conservative Estimate**: 20-50x faster for typical operations  
**Optimistic Estimate**: 100-200x faster for filter-heavy operations

**Most Impactful Single Change**: Vectorizing filter operations (#1) - **50-100x faster**

