import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('stock_data.db')

# Test 1: Check total records
print("=== TEST 1: Record Count ===")
count = pd.read_sql_query("SELECT COUNT(*) as total FROM stock_daily_data", conn)
print(f"Total records: {count['total'][0]}")
print(f"Expected: 300\n")

# Test 2: Check for duplicates
print("=== TEST 2: Duplicate Check ===")
duplicates = pd.read_sql_query("""
    SELECT symbol, date, COUNT(*) as count 
    FROM stock_daily_data 
    GROUP BY symbol, date 
    HAVING COUNT(*) > 1
""", conn)
print(f"Duplicate records: {len(duplicates)}")
print(f"Expected: 0\n")

# Test 3: Verify daily_change_percentage calculation
print("=== TEST 3: Calculation Verification ===")
sample = pd.read_sql_query("""
    SELECT symbol, date, open_price, close_price, daily_change_percentage
    FROM stock_daily_data 
    LIMIT 5
""", conn)
print(sample)
print("\nManual verification:")
for _, row in sample.iterrows():
    expected = ((row['close_price'] - row['open_price']) / row['open_price']) * 100
    print(f"{row['symbol']} {row['date']}: "
          f"Stored={row['daily_change_percentage']:.2f}%, "
          f"Calculated={expected:.2f}% - "
          f"{'PASS' if abs(row['daily_change_percentage'] - expected) < 0.01 else 'FAIL'}")

# Test 4: Check for NULL values
print("\n=== TEST 4: NULL Value Check ===")
nulls = pd.read_sql_query("""
    SELECT 
        SUM(CASE WHEN open_price IS NULL THEN 1 ELSE 0 END) as null_opens,
        SUM(CASE WHEN close_price IS NULL THEN 1 ELSE 0 END) as null_closes,
        SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volumes
    FROM stock_daily_data
""", conn)
print(nulls)
print("Expected: All zeros\n")

# Test 5: Verify data ranges
print("=== TEST 5: Data Ranges ===")
ranges = pd.read_sql_query("""
    SELECT 
        symbol,
        COUNT(*) as records,
        MIN(date) as first_date,
        MAX(date) as last_date,
        MIN(close_price) as min_price,
        MAX(close_price) as max_price
    FROM stock_daily_data
    GROUP BY symbol
""", conn)
print(ranges.to_string(index=False))

conn.close()
print("\n=== ALL TESTS COMPLETE ===")