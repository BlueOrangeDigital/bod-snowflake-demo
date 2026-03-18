#!/usr/bin/env python3
"""
SEC EDGAR Filings Ingestion
Fetches recent 8-K (M&A) and S-1 (IPO) filings from SEC EDGAR API
Uploads to Snowflake for Cortex AI processing
"""

import os
import sys
import requests
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
from bs4 import BeautifulSoup
import time

# SEC EDGAR API configuration
# API Docs: https://www.sec.gov/edgar/sec-api-documentation
# No API key required, but User-Agent header is mandatory
EDGAR_BASE = "https://www.sec.gov"
EDGAR_SEARCH = f"{EDGAR_BASE}/cgi-bin/browse-edgar"
EDGAR_ARCHIVES = f"{EDGAR_BASE}/Archives/edgar/data"

# Required headers for SEC API (they block requests without proper User-Agent)
# Format: "Company Name contact@email.com"
# TODO: Update with your company info for production use
HEADERS = {
    "User-Agent": "Blue Orange Digital Demo carson@blueorange.digital"
}

print("SEC EDGAR API: No API key needed!")
print(f"Using User-Agent: {HEADERS['User-Agent']}")
print("For production, update HEADERS in this script with your contact info.\n")

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="INGESTION_WH",
        database="AI_CORTEX_DEMO",
        schema="RAW_DATA"
    )

def fetch_recent_filings(form_type: str = "8-K", count: int = 50):
    """
    Fetch recent SEC filings of a specific type
    
    Args:
        form_type: '8-K' (M&A, material events) or 'S-1' (IPO registration)
        count: Number of filings to fetch
    
    Returns:
        List of dicts with filing metadata and text
    """
    # SEC EDGAR RSS feed for recent filings
    # Alternative: Use SEC's full-text search API
    rss_url = f"{EDGAR_BASE}/cgi-bin/browse-edgar?action=getcurrent&CIK=&type={form_type}&company=&dateb=&owner=exclude&start=0&count={count}&output=atom"
    
    print(f"Fetching recent {form_type} filings from SEC EDGAR...")
    
    response = requests.get(rss_url, headers=HEADERS)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, "xml")
    entries = soup.find_all("entry")
    
    filings = []
    
    for i, entry in enumerate(entries[:count]):
        try:
            # Extract metadata
            cik = entry.find("filing-href").text.split("/")[-2] if entry.find("filing-href") else "UNKNOWN"
            company_name = entry.find("title").text if entry.find("title") else "UNKNOWN"
            filing_date = entry.find("filing-date").text if entry.find("filing-date") else datetime.now().strftime("%Y-%m-%d")
            filing_url = entry.find("filing-href").text if entry.find("filing-href") else ""
            
            # Extract filing text (summary or full document)
            summary = entry.find("summary").text if entry.find("summary") else ""
            
            # Try to fetch full filing text (optional, can be large)
            filing_text = summary  # Start with summary
            
            # For demo purposes, we'll use the summary text
            # In production, you'd fetch the full HTML/TXT filing and extract relevant sections
            
            filings.append({
                "cik": cik,
                "company_name": company_name,
                "form_type": form_type,
                "filing_date": filing_date,
                "filing_text": filing_text[:10000],  # Limit to 10K chars for demo
                "url": filing_url,
                "ingested_at": datetime.now().isoformat()
            })
            
            print(f"  [{i+1}/{len(entries)}] {company_name} ({filing_date})")
            
            # Rate limiting: SEC allows ~10 requests/second
            time.sleep(0.2)
            
        except Exception as e:
            print(f"  ✗ Error parsing filing: {e}")
            continue
    
    return filings

def load_to_snowflake(filings: list, table_name: str = "SEC_FILINGS"):
    """Load filings to Snowflake table"""
    if not filings:
        print("No filings to load")
        return
    
    df = pd.DataFrame(filings)
    
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
            
            # Merge into main table (upsert on CIK + form_type + filing_date)
            cursor.execute(f"""
            MERGE INTO {table_name} AS target
            USING {table_name}_TEMP AS source
            ON target.CIK = source.CIK
               AND target.FORM_TYPE = source.FORM_TYPE
               AND target.FILING_DATE = source.FILING_DATE
            WHEN NOT MATCHED THEN
                INSERT (CIK, COMPANY_NAME, FORM_TYPE, FILING_DATE, FILING_TEXT, URL, INGESTED_AT)
                VALUES (source.CIK, source.COMPANY_NAME, source.FORM_TYPE, source.FILING_DATE, source.FILING_TEXT, source.URL, source.INGESTED_AT)
            """)
            
            merge_result = cursor.fetchone()
            print(f"Merge result: {merge_result[0]} new filings inserted")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    """Main ingestion flow"""
    print("SEC EDGAR Filings Ingestion")
    print("=" * 50)
    
    # Fetch 8-K filings (M&A announcements, material events)
    print("\nFetching 8-K filings (M&A, material events)...")
    filings_8k = fetch_recent_filings(form_type="8-K", count=30)
    
    print(f"\nFetched {len(filings_8k)} 8-K filings")
    
    if filings_8k:
        # Preview first filing
        print("\nSample filing:")
        sample = filings_8k[0]
        print(f"  Company: {sample['company_name']}")
        print(f"  Date: {sample['filing_date']}")
        print(f"  Text preview: {sample['filing_text'][:200]}...")
        
        # Load to Snowflake
        print("\nLoading 8-K filings to Snowflake...")
        load_to_snowflake(filings_8k)
    
    # Optional: Fetch S-1 filings (IPO registrations)
    # Uncomment if you want IPO data as well
    # print("\nFetching S-1 filings (IPO registrations)...")
    # filings_s1 = fetch_recent_filings(form_type="S-1", count=20)
    # if filings_s1:
    #     load_to_snowflake(filings_s1)
    
    print("\n✓ SEC filings ingestion complete!")

if __name__ == "__main__":
    main()
