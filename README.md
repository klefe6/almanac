# Almanac Futures v2.0 - Intraday Pattern Analysis

## Overview
This application analyzes historical intraday patterns in futures contracts to identify repeatable behaviors based on previous day conditions and time-of-day statistics.

**Version 2.0** features a complete architectural refactoring with modular structure, improved data validation, caching, and comprehensive test coverage.

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Application
```bash
python runalmanac.py
```

Or with custom settings:
```bash
python runalmanac.py --port 8086 --no-debug
```

The application will be available at: `http://127.0.0.1:8085`

### 3. Run Tests
```bash
pytest tests/
```

---

## Features

### Core Analysis
- **Monthly Statistics**: Performance analysis across 12 months (January-December)
- **Daily Statistics**: Day-of-week patterns (Monday-Sunday performance)
- **Hourly Statistics**: Average and variance of returns and ranges by hour (0-23)
- **Minute Statistics**: Drill-down analysis for specific hours and minutes
- **Multi-Scenario Filtering**: Combine multiple filter conditions with AND/OR logic
- **HOD/LOD Detection**: High-of-day and low-of-day timing analysis
- **Time Comparisons**: Filter based on price relationships at specific times
- **Dynamic Titles**: All graphs display product, case count, and date range

### Statistical Measures
Each analysis includes 5 measures:
- **Mean**: Average value
- **Trimmed Mean**: Average excluding top/bottom 5%
- **Median**: Middle value
- **Mode**: Most frequent value
- **Outlier**: Average of extremes

### Data Quality
- ✅ Trading calendar with holiday handling
- ✅ Data validation (nulls, duplicates, OHLC relationships)
- ✅ Extreme outlier detection and trimming
- ✅ Missing data gap reporting

### Performance
- ✅ Flask-caching for fast repeated queries
- ✅ Database connection pooling
- ✅ Efficient filtering pipeline

### Testing
- ✅ Comprehensive pytest suite
- ✅ Mock fixtures for database-free testing
- ✅ Edge case coverage

---

## Architecture

The application follows a clean, modular architecture:

```
almanac/
├── data_sources/    # Database loaders with validation
├── features/        # Statistical computations & filtering
├── viz/             # Plotly figure factory functions
└── pages/           # Dash UI layouts & callbacks
```

**See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed documentation.**

---

## Usage Guide

### Product Selection
Choose from:
- **ES**: E-mini S&P 500
- **NQ**: E-mini Nasdaq 100
- **GC**: Gold Futures

### Date Range
Select start and end dates for the analysis period.

### Filters & Scenarios

#### Scenario Builder
**Active Scenarios** panel allows you to build complex filter combinations:
- Add multiple scenarios with "Apply Scenario" button
- Each scenario shows number of matching days
- Use AND/OR logic dropdown to combine scenarios
- Remove scenarios with the X button
- **Note**: Mutually exclusive filters (e.g., Bullish AND Bearish) will return 0 cases

**Pre-configured Presets:**
- Bullish Days (Close > Open)
- Bearish Days (Close < Open)
- Strong Moves (high %∆)
- High Volume Days
- Weekdays Only
- All Conditions (no filters)

#### Previous Day Conditions
- **Close > Open / Close < Open**: Filter by previous day direction
- **%∆ Thresholds**: Set percentage change requirements (e.g., "previous day up >1%")
- **Relative Volume**: Filter by volume relative to 10-day average

#### Weekday Selection
Analyze specific days of the week (e.g., "only Mondays", "only Fridays")

#### Time Comparisons
Compare prices at two specific times to filter for intraday patterns
- Example: "Days when 9:45 price > 10:15 price"

#### Extreme Trimming
Exclude the top and bottom 5% of returns and ranges to focus on typical behavior

#### Save/Load Presets
- Save frequently used filter combinations
- Load saved presets for quick access
- Delete presets when no longer needed

### Output Charts (16 Total Graphs)

**Monthly Statistics** (4 graphs):
1. Average % Change by month (with 5 statistical measures)
2. Variance of % Change by month
3. Average Range by month (with 5 statistical measures)
4. Variance of Range by month

**Daily Statistics** (4 graphs):
5. Average % Change by day of week
6. Variance of % Change by day of week
7. Average Range by day of week
8. Variance of Range by day of week

**Hourly Statistics** (4 graphs):
9. Average % Change by hour
10. Variance of % Change by hour
11. Average Range by hour
12. Variance of Range by hour

**Minute Statistics** (4 graphs, for selected hour):
13. Average % Change by minute
14. Variance of % Change by minute
15. Average Range by minute
16. Variance of Range by minute

**All graphs display dynamic metadata:**
- Product symbol (e.g., ES, NQ, GC)
- Number of cases/days included
- Date range (start to end)

**HOD/LOD Analysis:**
- Survival curves (probability by time)
- Frequency heatmaps by weekday
- Rolling median time charts
- Trend indicators

**Summary Panel:**
- Total cases/days analyzed
- Active scenario filters with day counts
- Filtered data statistics
- Performance metrics

---

## Database Requirements

### Connection
The application connects to the RESEARCH database on SQL Server.

**Ensure you have:**
- ODBC Driver 17 for SQL Server installed
- Proper database access credentials
- Network access to the database server

### Required Indexes (for performance)
```sql
CREATE INDEX idx_raw_time_product 
  ON RawIntradayData(contract_id, interval, time);

CREATE INDEX idx_daily_product 
  ON DailyData(contract_id, time);
```

### Tables Used
- `dbo.RawIntradayData`: 1-minute OHLCV data
- `dbo.DailyData`: Daily OHLCV data

---

## Development

### Running Tests
```bash
# Run all tests
pytest tests/

# Verbose output
pytest tests/ -v

# With coverage report
pytest tests/ --cov=almanac --cov-report=html
```

### Code Formatting
```bash
# Format code
black almanac/

# Check linting
flake8 almanac/
```

### Project Structure
```
Almanac Futures/
├── almanac/              # Main package
│   ├── data_sources/     # Data loading & validation
│   ├── features/         # Statistics & filtering
│   ├── viz/              # Visualization
│   └── pages/            # UI components
├── tests/                # Test suite
├── .cache/               # Caching directory (auto-created)
├── runalmanac.py         # Application launcher
├── requirements.txt      # Python dependencies
├── pytest.ini            # Test configuration
├── ARCHITECTURE.md       # Detailed architecture docs
└── README.md             # This file
```

---

## Troubleshooting

### "No data found" Error
- **Check:** Product code spelling (ES, NQ, GC)
- **Check:** Date range has available data
- **Check:** Database connection and permissions

### Slow Performance
- **Solution:** Add database indexes (see above)
- **Solution:** Reduce date range (<3 months for minute data)
- **Solution:** Clear cache: delete `.cache/` directory

### Test Failures
- **Missing scipy:** Some statistical tests require `scipy`
- **Database mocks:** Integration tests use mocks by default

---

## Roadmap

### Phase 1 (Current - v2.0)
- ✅ Modular architecture
- ✅ Data validation & quality checks
- ✅ Trading calendar integration
- ✅ Flask-caching
- ✅ Comprehensive test suite

### Phase 2 (Current - v2.1)
- ✅ Monthly and Daily statistics
- ✅ Multi-scenario filtering with AND/OR logic
- ✅ Dynamic chart titles with metadata
- ✅ HOD/LOD survival curves and heatmaps
- ✅ Export functionality (CSV, PNG)
- ✅ Shareable URLs with query parameters
- ✅ Preset save/load
- ✅ Individual accordion sections for each calculation type

### Phase 3 (Future)
- [ ] Cross-asset correlation analysis
- [ ] Event calendar integration (CPI, NFP, FOMC) - partially implemented
- [ ] Strategy sandbox (simple backtests)
- [ ] ML classification (day archetype prediction)
- [ ] Quick preset dropdowns per calculation button

**See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed roadmap.**

---

## Contributing

1. Write tests for new features
2. Follow PEP 8 style guidelines
3. Update documentation
4. Run test suite before committing

---

## License

**Version:** 2.1.0  
**Author:** Kevin Lefebvre  
**License:** Internal Use Only
**Last Updated:** December 2024
