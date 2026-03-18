# Quick Start Guide
## Snowflake AI & Cortex Demo

**⏱️ Get running in 10 minutes**

---

## Prerequisites

```bash
# Verify installations
tofu version          # or: terraform version
python --version      # 3.9+
snowsql --version     # optional but recommended
```

---

## 1. Set Environment Variables (2 min)

```bash
export SNOWFLAKE_ACCOUNT="<your-account>"
export SNOWFLAKE_USER="<your-username>"
export SNOWFLAKE_PASSWORD="<your-password>"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"

# Optional: Alpha Vantage API key (get free from alphavantage.co)
export ALPHA_VANTAGE_API_KEY="demo"
```

---

## 2. Deploy Infrastructure (3 min)

```bash
tofu init
tofu apply -auto-approve
```

**Creates:**
- Database: `AI_CORTEX_DEMO`
- 3 Warehouses: `INGESTION_WH`, `ML_WH`, `CORTEX_WH`
- 4 Schemas: `RAW_DATA`, `ML_MODELS`, `CORTEX_AI`, `DASHBOARDS`
- 7 Tables

---

## 3. Install Python Dependencies (1 min)

```bash
pip install -r requirements.txt
```

---

## 4. Ingest Data (3 min)

```bash
# Stocks (10 symbols × 100 days = ~1,000 rows)
python ingest/fetch_stock_prices.py

# SEC filings (30 recent 8-K forms)
python ingest/fetch_sec_filings.py
```

---

## 5. Run AI Pipelines (2 min)

```bash
# Option A: Using snowsql
snowsql -c demo -f sql/ml_pipeline.sql
snowsql -c demo -f sql/cortex_pipeline.sql

# Option B: Copy/paste into Snowsight SQL worksheet
```

---

## 6. View Results (Immediate)

Open **Snowsight** → Navigate to `AI_CORTEX_DEMO` database

**Queries to try:**

```sql
-- ML predictions
SELECT * FROM ML_MODELS.STOCK_PREDICTIONS WHERE SYMBOL = 'AAPL';

-- Cortex AI summaries
SELECT COMPANY_NAME, AI_SUMMARY, SENTIMENT FROM CORTEX_AI.FILING_SUMMARIES LIMIT 10;

-- Dashboards
SELECT * FROM DASHBOARDS.STOCK_PREDICTION_DASHBOARD WHERE SYMBOL = 'AAPL' ORDER BY DATE DESC LIMIT 30;
SELECT * FROM DASHBOARDS.CORTEX_AI_DASHBOARD ORDER BY FILING_DATE DESC LIMIT 20;
```

---

## 🎬 Ready to Record?

See `demo/VIDEO-SCRIPT.md` for the complete 3-5 minute recording guide.

---

## Troubleshooting

**Q: Cortex functions not available?**  
A: Run: `GRANT USE AI FUNCTIONS ON ACCOUNT TO ROLE ACCOUNTADMIN;`

**Q: Alpha Vantage rate limit?**  
A: Free tier = 25 requests/day. Script includes 12-sec delays. Upgrade for faster ingestion.

**Q: Python import errors?**  
A: Run `pip install -r requirements.txt` again

---

## Cost Estimate

- **Snowflake compute:** ~$15/day (dev usage)
- **Cortex AI tokens:** ~$0.03 (50 filings)
- **Data sources:** $0 (all free public APIs)

---

**Full documentation:** See `README.md`  
**Support:** Carson @ #helm-team
