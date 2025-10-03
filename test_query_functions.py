from stock_etl_pipeline import query_stock_data, get_database_stats

# Test filtering by symbol
aapl_data = query_stock_data(symbol="AAPL")
print(f"AAPL records: {len(aapl_data)}")

# Test date range filtering
recent = query_stock_data(start_date="2025-09-01")
print(f"September+ records: {len(recent)}")

# View stats
get_database_stats()