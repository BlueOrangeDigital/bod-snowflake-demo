#!/usr/bin/env python3
"""
Stock Price Data Ingestion
Fetches real-time stock data from Alpha Vantage API
Uploads to Snowflake via Snowpipe or COPY INTO
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
from typing import List, Dict

# Alpha Vantage API configuration
# Get your free API key: https://www.alphavantage.co/support/#api-key
# Free tier: 25 requests/day, 5 requests/minute
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
BASE_URL = "https://www.alphavantage.co/query"

if API_KEY == "demo":
    print("⚠️  WARNING: Using demo API key (limited data)")
    print("   Get your free key: https://www.alphavantage.co/support/#api-key")
    print("   Then set: export ALPHA_VANTAGE_API_KEY='your-key'\n")

# Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=f"{os.getenv('SNOWFLAKE_ORGANIZATION_NAME')}-{os.getenv('SNOWFLAKE_ACCOUNT_NAME')}",
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="INGESTION_WH",
        database="AI_CORTEX_DEMO",
        schema="RAW_DATA"
    )

def fetch_daily_stock_data(symbol: str, outputsize: str = "compact") -> pd.DataFrame:
    """
    Fetch daily time series for a stock symbol
    
    Args:
        symbol: Stock ticker (e.g., 'AAPL')
        outputsize: 'compact' (100 days) or 'full' (20+ years)
    
    Returns:
        DataFrame with columns: symbol, date, open, high, low, close, volume
    """
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": outputsize
    }
    
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    
    if "Error Message" in data:
        print(f"API Error for {symbol}: {data['Error Message']}")
        return pd.DataFrame()
    
    if "Note" in data:
        print(f"API Rate Limit: {data['Note']}")
        return pd.DataFrame()
    
    if "Time Series (Daily)" not in data:
        print(f"Unexpected response for {symbol}: {data}")
        return pd.DataFrame()
    
    time_series = data["Time Series (Daily)"]
    
    rows = []
    for date_str, values in time_series.items():
        rows.append({
            "symbol": symbol,
            "date": date_str,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"]),
            "ingested_at": datetime.now().isoformat()
        })
    
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    
    return df

def load_to_snowflake(df: pd.DataFrame, table_name: str = "STOCK_PRICES"):
    """Load DataFrame to Snowflake table"""
    if df.empty:
        print("No data to load")
        return
    
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df.columns = df.columns.str.upper()

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        # Create temporary staging table
        cursor.execute(f"""
        CREATE TEMPORARY TABLE {table_name}_TEMP LIKE {table_name}
        """)
        
        # Write DataFrame to temp table
        from snowflake.connector.pandas_tools import write_pandas
        
        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=f"{table_name}_TEMP",
            auto_create_table=False,
            overwrite=False
        )
        
        if success:
            print(f"Loaded {num_rows} rows to temp table")
            
            # Merge into main table (upsert on symbol + date)
            cursor.execute(f"""
            MERGE INTO {table_name} AS target
            USING {table_name}_TEMP AS source
            ON target.SYMBOL = source.SYMBOL AND target.DATE = source.DATE
            WHEN MATCHED THEN
                UPDATE SET
                    target.OPEN = source.OPEN,
                    target.HIGH = source.HIGH,
                    target.LOW = source.LOW,
                    target.CLOSE = source.CLOSE,
                    target.VOLUME = source.VOLUME,
                    target.INGESTED_AT = source.INGESTED_AT
            WHEN NOT MATCHED THEN
                INSERT (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, INGESTED_AT)
                VALUES (source.SYMBOL, source.DATE, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.VOLUME, source.INGESTED_AT)
            """)
            
            merge_result = cursor.fetchone()
            print(f"Merge result: {merge_result[0]} rows inserted, {merge_result[1]} rows updated")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    """Main ingestion flow"""
    # Symbols to track (tech + finance)
    symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA", "JPM", "GS", "BAC", "WFC", "C"]
    
    print(f"Fetching stock data for {len(symbols)} symbols...")
    
    all_data = []
    for i, symbol in enumerate(symbols):
        print(f"[{i+1}/{len(symbols)}] Fetching {symbol}...")
        
        df = fetch_daily_stock_data(symbol, outputsize="compact")
        
        if not df.empty:
            all_data.append(df)
            print(f"  ✓ Fetched {len(df)} days of data")
        else:
            print(f"  ✗ No data returned")
        
        # Rate limiting: Alpha Vantage free tier = 25 requests/day, 5 requests/minute
        # Sleep 12 seconds between requests to stay under 5/minute
        if i < len(symbols) - 1:
            import time
            time.sleep(12)
    
    if not all_data:
        print("No data fetched. Exiting.")
        return
    
    # Combine all dataframes
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal rows fetched: {len(combined_df)}")
    print(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
    
    # Load to Snowflake
    print("\nLoading data to Snowflake...")
    load_to_snowflake(combined_df)
    
    print("\n✓ Ingestion complete!")

if __name__ == "__main__":
    main()
