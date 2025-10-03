# Stock Market ETL Pipeline

A production-ready Python ETL (Extract, Transform, Load) pipeline that automates the collection, processing, and storage of daily stock market data for Apple (AAPL), Google (GOOG), and Microsoft (MSFT).

## Project Overview

This project demonstrates a complete data engineering workflow, implementing industry best practices for data pipeline development. The pipeline connects to the Alpha Vantage API, retrieves historical and daily stock market data, validates and transforms it, and loads it into a structured database with built-in duplicate prevention.

### Implementation Approach

The project was built following a modular, test-driven approach:

1. **API Integration**: Implemented robust API calls with error handling for rate limits, network failures, and malformed responses
2. **Data Lake Storage**: Raw JSON responses are persisted to local storage before transformation, enabling data lineage tracking and reprocessing capabilities
3. **Data Validation**: Integrated Pydantic models to enforce schema validation at the transformation stage, ensuring data quality before database insertion
4. **Database Design**: Created a normalized SQLite database with proper constraints (UNIQUE on symbol/date combination) to maintain data integrity
5. **Automation**: Implemented flexible scheduling options (Python schedule library, Windows Task Scheduler, cron) for daily execution
6. **Testing & Verification**: Developed comprehensive test suite to validate data accuracy, duplicate prevention, and calculation correctness
7. **Logging & Monitoring**: Implemented multi-level logging (console + file) for operational visibility and debugging

This architecture ensures the pipeline is maintainable, scalable, and production-ready.

## Features

- **Extract**: Fetches stock data from Alpha Vantage API and saves raw JSON files
- **Transform**: Cleans and processes data with Pydantic validation
- **Load**: Stores data in SQLite database with duplicate prevention
- **Data Validation**: Uses Pydantic models to ensure data integrity
- **Scheduling**: Automated daily execution options
- **Logging**: Comprehensive logging to file and console

## Requirements

- Python 3.7+
- Alpha Vantage API key (free at https://www.alphavantage.co/support/#api-key)

## Installation

1. Clone the repository:
```bash
git clone <your-repo-url>
cd <your-repo-name>
```

2. Install required packages:
```bash
pip install pandas requests pydantic schedule
```

3. Configure your API key:
   - Open `stock_etl_pipeline.py`
   - Replace `YOUR_API_KEY_HERE` on line 25 with your actual Alpha Vantage API key

## Usage

### Run Once
Execute the pipeline manually:
```bash
python stock_etl_pipeline.py
```

### Automated Scheduling

#### Option 1: Python Schedule Library (Built-in)
The script includes built-in scheduling using the `schedule` library.

1. Uncomment line 358 in `stock_etl_pipeline.py`:
```python
schedule_daily_run()  # Remove the # before this line
```

2. Run the script:
```bash
python stock_etl_pipeline.py
```

The script will run continuously and execute the ETL pipeline daily at 6:00 PM.

**Note**: The terminal must remain open for this to work.

#### Option 2: Windows Task Scheduler (Recommended)
For production use on Windows, use Task Scheduler for more reliable automation:

1. Open Task Scheduler (search in Windows Start menu)
2. Click "Create Basic Task"
3. Configure the task:
   - **Name**: Stock ETL Pipeline
   - **Trigger**: Daily at 6:00 PM
   - **Action**: Start a program
   - **Program**: Full path to python.exe
     - Example: `C:\Users\YourUsername\AppData\Local\Programs\Python\Python313\python.exe`
   - **Arguments**: `stock_etl_pipeline.py`
   - **Start in**: Full path to your project folder
     - Example: `C:\Users\YourUsername\Desktop\Sweeft-task`

**Advantages**:
- Runs even when terminal is closed
- Runs even after system restart
- Windows manages the schedule automatically

#### Option 3: Linux/macOS (cron)
For Unix-based systems, use cron:

1. Edit crontab:
```bash
crontab -e
```

2. Add this line to run daily at 6:00 PM:
```bash
0 18 * * * /usr/bin/python3 /path/to/stock_etl_pipeline.py
```

## Project Structure

```
project/
├── stock_etl_pipeline.py       # Main ETL script
├── test_database.py            # Database integrity tests
├── test_query_functions.py     # Query function tests
├── verify_json_files.py        # JSON file verification
├── README.md                   # Project documentation
├── stock_data.db               # SQLite database (created automatically)
├── etl_pipeline.log            # Log file (created automatically)
└── raw_data/                   # Raw JSON files (created automatically)
    ├── AAPL_2025-10-03.json
    ├── GOOG_2025-10-03.json
    └── MSFT_2025-10-03.json
```

## Database Schema

Table: `stock_daily_data`

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key (auto-increment) |
| symbol | TEXT | Stock symbol (AAPL, GOOG, MSFT) |
| date | DATE | Trading date |
| open_price | REAL | Opening price |
| high_price | REAL | Highest price |
| low_price | REAL | Lowest price |
| close_price | REAL | Closing price |
| volume | INTEGER | Trading volume |
| daily_change_percentage | REAL | Daily percentage change |
| extraction_timestamp | TIMESTAMP | When data was extracted |

**Constraint**: UNIQUE(symbol, date) - prevents duplicate entries

## Data Validation

The pipeline uses Pydantic for robust data validation:

```python
class StockDailyData(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
```

Invalid data is logged and skipped, ensuring only clean data enters the database.

## Querying the Database

### Built-in Query Functions

```python
from stock_etl_pipeline import query_stock_data, get_database_stats

# Query by symbol
aapl_data = query_stock_data(symbol="AAPL")

# Query by date range
recent = query_stock_data(start_date="2025-09-01", end_date="2025-10-02")

# Get database statistics
get_database_stats()
```

### Direct SQL Queries

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('stock_data.db')
df = pd.read_sql_query("SELECT * FROM stock_daily_data WHERE symbol='AAPL'", conn)
conn.close()
```

## Logging

All operations are logged to:
- Console output (real-time)
- `etl_pipeline.log` file (persistent)

Log levels:
- INFO: Normal operations
- WARNING: Non-critical issues (e.g., API rate limits)
- ERROR: Failures in extraction, transformation, or loading

## API Rate Limits

Alpha Vantage free tier limits:
- 5 API calls per minute
- 500 API calls per day

The pipeline includes 12-second delays between requests to comply with these limits.

## Testing

Run the included test scripts to verify functionality:

```bash
# Test database integrity
python test_database.py

# Test query functions
python test_query_functions.py

# Verify JSON files
python verify_json_files.py
```

## Troubleshooting

### "Error binding parameter: type 'Timestamp' is not supported"
This error has been fixed in the current version. If you encounter it, ensure you're using the latest code.

### "API Rate Limit" or "Note" in response
Wait 1 minute before running again. The free tier allows 5 calls per minute.

### No data loaded
Check that your API key is correctly set in the script.


- Data provided by [Alpha Vantage](https://www.alphavantage.co/)
