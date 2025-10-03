import os
import json
import requests
import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from pydantic import BaseModel, ValidationError, Field
import schedule
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
API_KEY = "GJTXXPI8TS0JXX56"  # Replace with your actual API key
BASE_URL = "https://www.alphavantage.co/query"
SYMBOLS = ["AAPL", "GOOG", "MSFT"]
RAW_DATA_DIR = Path("raw_data")
DB_PATH = "stock_data.db"

# Create directories if they don't exist
RAW_DATA_DIR.mkdir(exist_ok=True)

# ==================== PYDANTIC MODELS (Bonus) ====================
class StockDailyData(BaseModel):
    """Pydantic model for validating stock data"""
    date: str
    open: float = Field(alias="1. open")
    high: float = Field(alias="2. high")
    low: float = Field(alias="3. low")
    close: float = Field(alias="4. close")
    volume: int = Field(alias="5. volume")
    
    class Config:
        populate_by_name = True


# ==================== EXTRACT ====================
def extract_stock_data(symbol: str, save_raw: bool = True) -> Optional[Dict]:
    """
    Extract stock data from Alpha Vantage API
    
    Args:
        symbol: Stock symbol (e.g., 'AAPL')
        save_raw: Whether to save raw JSON response
        
    Returns:
        Dict containing the API response or None if failed
    """
    logger.info(f"Extracting data for {symbol}...")
    
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact"  # Last 100 data points
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if "Error Message" in data:
            logger.error(f"API Error for {symbol}: {data['Error Message']}")
            return None
        
        if "Note" in data:
            logger.warning(f"API Rate Limit: {data['Note']}")
            return None
        
        # Save raw data
        if save_raw:
            today = datetime.now().strftime("%Y-%m-%d")
            filename = RAW_DATA_DIR / f"{symbol}_{today}.json"
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Raw data saved to {filename}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {symbol}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for {symbol}: {e}")
        return None


# ==================== TRANSFORM ====================
def transform_stock_data(raw_data: Dict, symbol: str, validate: bool = True) -> Optional[pd.DataFrame]:
    """
    Transform raw JSON data into structured DataFrame
    
    Args:
        raw_data: Raw API response
        symbol: Stock symbol
        validate: Whether to validate data with Pydantic
        
    Returns:
        Transformed DataFrame or None if transformation fails
    """
    logger.info(f"Transforming data for {symbol}...")
    
    try:
        # Extract time series data
        time_series_key = "Time Series (Daily)"
        if time_series_key not in raw_data:
            logger.error(f"No time series data found for {symbol}")
            return None
        
        time_series = raw_data[time_series_key]
        
        # Validate data with Pydantic (Bonus)
        if validate:
            validated_records = []
            for date, values in time_series.items():
                try:
                    values_with_date = {"date": date, **values}
                    validated = StockDailyData(**values_with_date)
                    validated_records.append(validated.model_dump(by_alias=False))
                except ValidationError as e:
                    logger.warning(f"Validation error for {symbol} on {date}: {e}")
                    continue
            
            if not validated_records:
                logger.error(f"No valid records found for {symbol}")
                return None
            
            # Convert validated records to DataFrame
            df = pd.DataFrame(validated_records)
        else:
            # Convert to DataFrame without validation
            df = pd.DataFrame.from_dict(time_series, orient='index')
            df.index.name = 'date'
            df.reset_index(inplace=True)
            
            # Rename columns
            df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        
        # Ensure proper data types
        df['date'] = pd.to_datetime(df['date'])
        df['open'] = pd.to_numeric(df['open'], errors='coerce')
        df['high'] = pd.to_numeric(df['high'], errors='coerce')
        df['low'] = pd.to_numeric(df['low'], errors='coerce')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').astype('Int64')
        
        # Add symbol column
        df['symbol'] = symbol
        
        # Calculate daily_change_percentage
        df['daily_change_percentage'] = ((df['close'] - df['open']) / df['open']) * 100
        df['daily_change_percentage'] = df['daily_change_percentage'].round(2)
        
        # Add extraction timestamp
        df['extraction_timestamp'] = datetime.now()
        
        # Remove any rows with NaN values
        df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
        
        logger.info(f"Transformed {len(df)} records for {symbol}")
        return df
        
    except Exception as e:
        logger.error(f"Transformation failed for {symbol}: {e}")
        return None


# ==================== LOAD ====================
def create_database_table(conn: sqlite3.Connection):
    """Create the stock_daily_data table if it doesn't exist"""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_daily_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            open_price REAL NOT NULL,
            high_price REAL NOT NULL,
            low_price REAL NOT NULL,
            close_price REAL NOT NULL,
            volume INTEGER,
            daily_change_percentage REAL,
            extraction_timestamp TIMESTAMP NOT NULL,
            UNIQUE(symbol, date)
        )
    """)
    conn.commit()
    logger.info("Database table created/verified")


def load_to_database(df: pd.DataFrame, db_path: str = DB_PATH):
    """
    Load transformed data into SQLite database
    
    Args:
        df: Transformed DataFrame
        db_path: Path to SQLite database
    """
    if df is None or df.empty:
        logger.warning("No data to load")
        return
    
    symbol = df['symbol'].iloc[0]
    logger.info(f"Loading data for {symbol} into database...")
    
    try:
        conn = sqlite3.connect(db_path)
        create_database_table(conn)
        
        # Prepare data for insertion
        df_to_load = df.copy()
        df_to_load.rename(columns={
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price'
        }, inplace=True)
        
        # Convert datetime columns to strings for SQLite compatibility
        df_to_load['date'] = df_to_load['date'].dt.strftime('%Y-%m-%d')
        df_to_load['extraction_timestamp'] = df_to_load['extraction_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Select only required columns
        columns_to_load = [
            'symbol', 'date', 'open_price', 'high_price', 
            'low_price', 'close_price', 'volume', 
            'daily_change_percentage', 'extraction_timestamp'
        ]
        df_to_load = df_to_load[columns_to_load]
        
        # Insert data with conflict resolution (ignore duplicates)
        inserted = 0
        skipped = 0
        
        for _, row in df_to_load.iterrows():
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO stock_daily_data 
                    (symbol, date, open_price, high_price, low_price, 
                     close_price, volume, daily_change_percentage, extraction_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, tuple(row))
                
                if conn.total_changes > 0:
                    inserted += 1
                else:
                    skipped += 1
                    
            except sqlite3.IntegrityError:
                skipped += 1
                continue
        
        conn.commit()
        conn.close()
        
        logger.info(f"Loaded {inserted} new records for {symbol}, skipped {skipped} duplicates")
        
    except Exception as e:
        logger.error(f"Database loading failed for {symbol}: {e}")


# ==================== MAIN ETL PIPELINE ====================
def run_etl_pipeline(symbols: List[str] = SYMBOLS):
    """
    Run the complete ETL pipeline for given symbols
    
    Args:
        symbols: List of stock symbols to process
    """
    logger.info("="*50)
    logger.info("Starting ETL Pipeline")
    logger.info("="*50)
    
    for symbol in symbols:
        try:
            # Extract
            raw_data = extract_stock_data(symbol)
            if raw_data is None:
                logger.warning(f"Skipping {symbol} due to extraction failure")
                continue
            
            # Transform
            df = transform_stock_data(raw_data, symbol, validate=True)
            if df is None or df.empty:
                logger.warning(f"Skipping {symbol} due to transformation failure")
                continue
            
            # Load
            load_to_database(df)
            
            logger.info(f"Successfully processed {symbol}")
            
            # Small delay to avoid hitting API rate limits
            time.sleep(12)  # Free tier allows 5 requests/minute
            
        except Exception as e:
            logger.error(f"Unexpected error processing {symbol}: {e}")
            continue
    
    logger.info("="*50)
    logger.info("ETL Pipeline Completed")
    logger.info("="*50)


# ==================== SCHEDULING (Bonus) ====================
def schedule_daily_run():
    """
    Schedule the ETL pipeline to run daily at 6 PM
    This is a blocking function that runs indefinitely
    """
    logger.info("Scheduling daily ETL runs at 11:43...")
    schedule.every().day.at("11:43").do(run_etl_pipeline)
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


# ==================== UTILITY FUNCTIONS ====================
def query_stock_data(symbol: str = None, start_date: str = None, end_date: str = None):
    """
    Query data from the database
    
    Args:
        symbol: Filter by stock symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with query results
    """
    conn = sqlite3.connect(DB_PATH)
    
    query = "SELECT * FROM stock_daily_data WHERE 1=1"
    params = []
    
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    
    query += " ORDER BY symbol, date DESC"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df


def get_database_stats():
    """Print statistics about the database"""
    conn = sqlite3.connect(DB_PATH)
    
    # Count by symbol
    df = pd.read_sql_query("""
        SELECT 
            symbol,
            COUNT(*) as record_count,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM stock_daily_data
        GROUP BY symbol
    """, conn)
    
    conn.close()
    
    print("\n" + "="*50)
    print("DATABASE STATISTICS")
    print("="*50)
    print(df.to_string(index=False))
    print("="*50 + "\n")


# ==================== MAIN ====================
if __name__ == "__main__":
    # Make sure to set your API key
    if API_KEY == "YOUR_API_KEY_HERE":
        print("ERROR: Please set your Alpha Vantage API key in the API_KEY variable")
        exit(1)
    
    # Run the ETL pipeline once
    run_etl_pipeline()
    
    # Display database statistics
    get_database_stats()
    
    # Example query
    print("\nExample: Latest 5 records for AAPL:")
    result = query_stock_data(symbol="AAPL")
    if not result.empty:
        print(result.head().to_string(index=False))
    
    # Uncomment to enable daily scheduling
    schedule_daily_run()