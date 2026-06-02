#!/bin/bash

# 1. Re-ingest raw data (Alpha Vantage + SEC EDGAR)
python ingest/fetch_stock_prices.py
python ingest/fetch_sec_filings.py

# 2. Rebuild ML + Cortex pipelines
snowsql -c demo -f sql/ml_pipeline.sql
snowsql -c demo -f sql/cortex_pipeline.sql