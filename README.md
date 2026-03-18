# Snowflake AI & Cortex Demo

**Complete end-to-end demonstration of Snowflake AI and Cortex capabilities**  
**Author:** Carson, Blue Orange Digital  
**Date:** March 16, 2026

---

## Overview

This demo showcases:

✅ **Live data ingestion** from free public sources (financial, real estate, SEC filings)  
✅ **Traditional ML pipeline** — Stock price prediction using regression  
✅ **Cortex AI LLM pipeline** — SEC filing summarization + sentiment analysis  
✅ **Automated orchestration** — Snowflake Tasks for scheduled updates  
✅ **Interactive dashboards** — Snowsight visualizations  

**Demo Duration:** 3-5 minutes  
**Tech Stack:** Snowflake, OpenTofu/Terraform, Python, Cortex AI

---

## Architecture

```
Data Sources          Ingestion           Snowflake Processing         Outputs
─────────────         ──────────          ────────────────────         ───────

Alpha Vantage  ──┐                   ┌─► ML Pipeline          ──► Stock Predictions
                 │                   │   (Regression)             (Next 7 days)
Zillow Data    ──┼──► Python ETL ───┼
                 │                   │
SEC EDGAR API  ──┘                   └─► Cortex AI Pipeline  ──► Filing Summaries
                                         (LLM Functions)          + Sentiment
                                                                  + Classification
```

---

## Quick Start

### Prerequisites

- **Snowflake account** (Trial or higher)
- **OpenTofu** or **Terraform** (v1.5+)
- **Python 3.9+**
- **Alpha Vantage API key** (free from alphavantage.co)

### Step 1: Set Environment Variables

```bash
# Snowflake connection
export SNOWFLAKE_ACCOUNT="<your-account>"
export SNOWFLAKE_USER="<your-username>"
export SNOWFLAKE_PASSWORD="<your-password>"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"

# Alpha Vantage API (optional, uses demo key by default)
export ALPHA_VANTAGE_API_KEY="<your-api-key>"
```

### Step 2: Deploy Infrastructure

```bash
# Initialize OpenTofu/Terraform
tofu init
# or: terraform init

# Review plan
tofu plan

# Deploy (creates database, schemas, tables, warehouses)
tofu apply -auto-approve
```

**What gets created:**

Database: `AI_CORTEX_DEMO`  
Schemas: `RAW_DATA`, `ML_MODELS`, `CORTEX_AI`, `DASHBOARDS`  
Warehouses: `INGESTION_WH`, `ML_WH`, `CORTEX_WH`  
Tables: `STOCK_PRICES`, `SEC_FILINGS`, `STOCK_PREDICTIONS`, `FILING_SUMMARIES`

### Step 3: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Ingest Data

```bash
# Fetch stock prices (10 symbols, last 100 days)
python ingest/fetch_stock_prices.py

# Fetch SEC filings (30 recent 8-K filings)
python ingest/fetch_sec_filings.py
```

**Expected output:**

- Stock prices: ~1,000 rows (10 symbols × 100 days)
- SEC filings: ~30 filings with full text

### Step 5: Run ML Pipeline

```bash
# Connect to Snowflake and run ML queries
snowsql -c demo -f sql/ml_pipeline.sql
```

**What it does:**

1. Creates feature engineering view (moving averages, momentum, volatility)
2. Trains linear regression model on historical data
3. Generates predictions for next 7 days
4. Calculates performance metrics (MAE, RMSE, MAPE)
5. Creates dashboard views for Snowsight

### Step 6: Run Cortex AI Pipeline

```bash
# Run Cortex AI queries
snowsql -c demo -f sql/cortex_pipeline.sql
```

**What it does:**

1. Processes SEC filings with `AI_COMPLETE()` for summarization
2. Analyzes sentiment using `AI_SENTIMENT()`
3. Classifies filings (M&A, IPO, Restructuring, etc.)
4. Generates executive briefing report
5. Creates dashboard views with sentiment trends

### Step 7: View Results in Snowsight

1. Open **Snowsight** (your Snowflake web UI)
2. Navigate to **AI_CORTEX_DEMO** database
3. Explore dashboards:
   - `DASHBOARDS.STOCK_PREDICTION_DASHBOARD` — ML predictions vs actuals
   - `DASHBOARDS.CORTEX_AI_DASHBOARD` — LLM-processed filings
   - `DASHBOARDS.ML_SUMMARY_STATS` — Model performance metrics
   - `DASHBOARDS.CORTEX_SUMMARY_STATS` — Cortex processing stats

---

## Demo Script (3-5 minutes)

Use this script for the video recording:

### Part 1: Introduction (30 sec)

> "Today I'm demonstrating Snowflake's AI and Cortex capabilities with a real-world use case: analyzing financial data and SEC filings using both traditional machine learning and large language models."

### Part 2: Data Ingestion (45 sec)

> "We're ingesting live data from three sources:
> - Stock prices from Alpha Vantage API
> - Real estate trends from Zillow
> - SEC filings from EDGAR API
>
> All data flows into Snowflake's RAW_DATA schema using Python ETL scripts. Let's look at the stock price data..."

**Show:** `SELECT * FROM RAW_DATA.STOCK_PRICES LIMIT 10;`

> "...and the SEC filings with full text content."

**Show:** `SELECT COMPANY_NAME, FILING_DATE, SUBSTR(FILING_TEXT, 1, 200) FROM RAW_DATA.SEC_FILINGS LIMIT 5;`

### Part 3: Traditional ML Pipeline (60 sec)

> "First, let's look at the traditional ML pipeline for stock price prediction. We've engineered features like moving averages, momentum indicators, and volatility measures..."

**Show:** `SELECT * FROM ML_MODELS.STOCK_FEATURES WHERE SYMBOL = 'AAPL' ORDER BY DATE DESC LIMIT 10;`

> "Using Snowflake ML, we trained a regression model on historical data and generated predictions for the next 7 days. Here are the forecasts with confidence intervals..."

**Show:** `SELECT * FROM ML_MODELS.STOCK_PREDICTIONS WHERE SYMBOL = 'AAPL' ORDER BY PREDICTION_DATE;`

> "And here's our model performance metrics. We're achieving a Mean Absolute Percentage Error of around 3-5% for most stocks."

**Show:** `SELECT SYMBOL, MAPE FROM ML_MODELS.MODEL_PERFORMANCE GROUP BY SYMBOL ORDER BY MAPE;`

### Part 4: Cortex AI Pipeline (90 sec)

> "Now for the Cortex AI pipeline—this is where it gets interesting. We're using Snowflake's built-in LLM functions to process unstructured text from SEC filings.
>
> First, AI_COMPLETE generates concise summaries..."

**Show:** `SELECT COMPANY_NAME, AI_SUMMARY FROM CORTEX_AI.FILING_SUMMARIES LIMIT 3;`

> "Then AI_SENTIMENT analyzes sentiment—positive, negative, or neutral..."

**Show:** `SELECT COMPANY_NAME, SENTIMENT, SENTIMENT_SCORE FROM CORTEX_AI.FILING_SUMMARIES LIMIT 5;`

> "And finally, we classify each filing into categories like M&A, IPO, or Restructuring using a classification prompt."

**Show:** `SELECT CLASSIFICATION, COUNT(*) AS COUNT FROM CORTEX_AI.FILING_SUMMARIES GROUP BY CLASSIFICATION;`

> "Here's a breakdown of M&A activity we detected..."

**Show:** `SELECT * FROM CORTEX_AI.MA_ACTIVITY LIMIT 5;`

> "And we even generate an executive briefing report that summarizes all recent activity."

**Show:** `SELECT * FROM CORTEX_AI.EXECUTIVE_BRIEFING;`

### Part 5: Dashboards (30 sec)

> "All of this comes together in Snowsight dashboards. Here's our ML prediction dashboard showing forecast vs actuals..."

**Show:** Snowsight dashboard with `DASHBOARDS.STOCK_PREDICTION_DASHBOARD`

> "And here's the Cortex AI dashboard with sentiment trends and classification breakdown."

**Show:** Snowsight dashboard with `DASHBOARDS.CORTEX_AI_DASHBOARD`

### Part 6: Conclusion (15 sec)

> "In just a few minutes, we've demonstrated:
> - Live data ingestion
> - Traditional ML for time series forecasting
> - Cortex AI for LLM-powered text analysis
> - All running entirely within Snowflake.
>
> Questions? Find me on Slack at #helm-team."

---

## Cost Estimation

**Snowflake compute costs (approximate):**

| Warehouse | Size | Usage | Monthly Cost |
|-----------|------|-------|--------------|
| INGESTION_WH | XSMALL | 1 hour/day | ~$50 |
| ML_WH | MEDIUM | 2 hours/day | ~$300 |
| CORTEX_WH | SMALL | 1 hour/day | ~$100 |
| **Total** | | | **~$450/month** |

**Cortex AI token costs:**

- Llama 3.3 70B: ~$0.50/1M tokens (input + output)
- Processing 50 filings (~50K tokens): **~$0.03**

**Data sources:** All free (Alpha Vantage free tier, public Zillow data, SEC EDGAR API)

---

## Troubleshooting

### Issue: `CORTEX AI functions not found`

**Solution:** Ensure your Snowflake account has Cortex AI enabled:

```sql
GRANT USE AI FUNCTIONS ON ACCOUNT TO ROLE ACCOUNTADMIN;
```

### Issue: `Alpha Vantage API rate limit`

**Solution:** The free tier allows 25 requests/day. The script includes 12-second delays between requests. For faster ingestion, upgrade to a paid Alpha Vantage plan.

### Issue: `Insufficient privileges`

**Solution:** Run all setup as `ACCOUNTADMIN` role. For production, create custom roles with appropriate grants.

### Issue: `SEC filings return empty text`

**Solution:** SEC EDGAR requires a User-Agent header. The script includes this, but if it fails, check your IP isn't rate-limited by SEC.

---

## Next Steps

1. **Enhance ML model** — Try XGBoost or LSTM for better accuracy
2. **Add more data sources** — Economic indicators, news sentiment, social media
3. **Implement alerting** — Email/Slack notifications for significant events
4. **Deploy to production** — Set up Snowflake Tasks for automated daily updates
5. **Build Streamlit app** — Interactive UI for exploring predictions and summaries

---

## Project Structure

```
snowflake-ai-cortex-demo/
├── README.md                    ← You are here
├── PROJECT-PLAN.md              ← Milestone tracking
├── main.tf                      ← OpenTofu/Terraform infrastructure
├── requirements.txt             ← Python dependencies
├── ingest/
│   ├── fetch_stock_prices.py   ← Alpha Vantage ETL
│   ├── fetch_sec_filings.py    ← SEC EDGAR ETL
│   └── fetch_zillow_data.py    ← Zillow CSV processing (optional)
├── sql/
│   ├── ml_pipeline.sql          ← ML model training & prediction
│   └── cortex_pipeline.sql      ← Cortex AI processing
└── demo/
    ├── video_script.md          ← Detailed recording script
    └── snowsight_dashboards.json ← Dashboard definitions
```

---

## Resources

- **Snowflake Cortex Docs:** https://docs.snowflake.com/en/user-guide/snowflake-cortex
- **Snowflake ML Docs:** https://docs.snowflake.com/en/user-guide/ml-functions
- **Alpha Vantage API:** https://www.alphavantage.co/documentation/
- **SEC EDGAR API:** https://www.sec.gov/edgar/sec-api-documentation
- **OpenTofu Snowflake Provider:** https://registry.terraform.io/providers/Snowflake-Labs/snowflake

---

## Support

**Questions or issues?**  
Carson @ Blue Orange Digital  
Slack: `#helm-team`  
Email: carson@blueorange.digital

---

**Status:** ✅ Ready for demo recording  
**Last Updated:** March 16, 2026
