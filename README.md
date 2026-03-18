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

## Two Approaches: SQL vs Snowpark

This demo provides **two implementations**:

| Approach | Files | Best For | Complexity |
|----------|-------|----------|------------|
| **SQL** | `sql/*.sql` | Quick demos, prototyping | ⭐ Simple |
| **Snowpark Python** | `snowpark/*.py` | Production ML, custom models | ⭐⭐⭐ Advanced |

**For quick demos (5-10 min):** Use the SQL approach  
**For production/technical audiences:** Use the Snowpark approach

📘 **Full comparison:** See [SNOWPARK-GUIDE.md](SNOWPARK-GUIDE.md)

---

## Prerequisites & Setup

### 1. Snowflake Account

**Get a free trial:** https://signup.snowflake.com/

- **Trial:** 30 days, $400 credits (more than enough for this demo)
- **Choose:** Standard edition or higher
- **Region:** Any (US East or West recommended)

### 2. OpenTofu (Infrastructure as Code)

**Download:** https://opentofu.org/docs/intro/install/

**Installation:**

**macOS (Homebrew):**
```bash
brew install opentofu
```

**Linux:**
```bash
# Standalone binary
curl --proto '=https' --tlsv1.2 -fsSL https://get.opentofu.org/install-opentofu.sh -o install-opentofu.sh
chmod +x install-opentofu.sh
./install-opentofu.sh --install-method standalone
```

**Windows (Chocolatey):**
```powershell
choco install opentofu
```

**Alternative:** Use Terraform instead → https://www.terraform.io/downloads (commands are identical, just replace `tofu` with `terraform`)

### 3. Python 3.9+

**Download:** https://www.python.org/downloads/

**Check version:**
```bash
python --version  # Should be 3.9 or higher
```

### 4. Alpha Vantage API Key (Stock Data)

**Get free API key:** https://www.alphavantage.co/support/#api-key

1. Click "Get Your Free API Key Today"
2. Fill in basic info (name, email)
3. **Free tier:** 25 requests/day, 5 requests/minute
4. Copy your API key (looks like: `ABCD1234EFGH5678`)

**Note:** The demo works with the free tier (fetches 10 stocks). For production or faster ingestion, upgrade to a paid plan.

### 5. SEC EDGAR API (SEC Filings)

**No API key required!** 🎉

- **Public API:** https://www.sec.gov/edgar/sec-api-documentation
- **Rate limit:** ~10 requests/second
- **Required:** User-Agent header (already included in our script)

**Important:** SEC requires a proper User-Agent header with contact info. Our script uses:
```
User-Agent: Blue Orange Digital Demo carson@blueorange.digital
```

**For production:** Update the header in `ingest/fetch_sec_filings.py` with your company name and email.

### 6. Zillow Research Data (Real Estate) - OPTIONAL

**Public CSV downloads:** https://www.zillow.com/research/data/

- **No API key needed** — CSV files are publicly available
- **Data types:** Home values (ZHVI), sales, inventory, rentals
- **Update frequency:** Monthly

**Note:** This demo currently uses stock prices and SEC filings. Zillow data is optional for future extensions.

---

## Getting Started Checklist

Before running the demo, complete these setup steps:

- [ ] **Snowflake account** — Sign up for free trial: https://signup.snowflake.com/
- [ ] **OpenTofu installed** — Download: https://opentofu.org/docs/intro/install/
- [ ] **Python 3.9+** — Download: https://www.python.org/downloads/
- [ ] **Alpha Vantage API key** — Get free key: https://www.alphavantage.co/support/#api-key
- [ ] **Clone this repo** — `git clone https://github.com/BlueOrangeDigital/bod-snowflake-demo.git`

**Time estimate:** ~10 minutes for full setup

---

## Quick Start

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

## Alternative: Snowpark Python Approach

For **production-grade ML** with custom models and better accuracy:

### Step 1: Install ML Dependencies

```bash
pip install -r requirements.txt
```

Includes: `snowflake-snowpark-python`, `xgboost`, `scikit-learn`

### Step 2: Run Snowpark ML Pipeline

```bash
python snowpark/ml_pipeline.py
```

**What it does:**
- Connects to Snowflake via Snowpark
- Feature engineering with Snowpark DataFrame API
- Trains **XGBoost model** (better accuracy than SQL approach)
- Hyperparameter tuning with GridSearchCV
- Stores predictions + creates stored procedure

**Expected output:**
```
=== Model Performance ===
MAE:  $2.34
RMSE: $3.12
MAPE: 2.87%  ← Much better than SQL approach (5-7%)
R²:   0.9412
```

### Step 3: Run Snowpark Cortex Pipeline

```bash
python snowpark/cortex_pipeline.py
```

**What it does:**
- Processes SEC filings with Cortex AI via Snowpark
- `AI_COMPLETE()` for summarization
- `AI_SENTIMENT()` for sentiment analysis
- Classification with Python logic
- Creates analytical views + executive briefing

**Benefits over SQL approach:**
✅ Better ML accuracy (XGBoost vs linear regression)  
✅ Full Python ML ecosystem (scikit-learn, custom models)  
✅ Production-ready (model registry, versioning, testing)  
✅ More flexible feature engineering  

📘 **Full guide:** See [SNOWPARK-GUIDE.md](SNOWPARK-GUIDE.md) for detailed comparison and production deployment.

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
├── SNOWPARK-GUIDE.md            ← Comprehensive Snowpark guide
├── PROJECT-PLAN.md              ← Milestone tracking
├── QUICK-START.md               ← 10-minute setup
├── main.tf                      ← OpenTofu/Terraform infrastructure
├── requirements.txt             ← Python dependencies
├── ingest/
│   ├── fetch_stock_prices.py   ← Alpha Vantage ETL
│   ├── fetch_sec_filings.py    ← SEC EDGAR ETL
│   └── fetch_zillow_data.py    ← Zillow CSV processing (optional)
├── sql/
│   ├── ml_pipeline.sql          ← ML model training (SQL approach)
│   └── cortex_pipeline.sql      ← Cortex AI processing (SQL approach)
├── snowpark/
│   ├── ml_pipeline.py           ← XGBoost ML (Snowpark approach)
│   └── cortex_pipeline.py       ← Cortex AI (Snowpark approach)
└── demo/
    └── VIDEO-SCRIPT.md          ← 3-5 minute recording script
```

---

## External APIs Reference

### Alpha Vantage (Stock Data)

| Property | Value |
|----------|-------|
| **Signup** | https://www.alphavantage.co/support/#api-key |
| **Docs** | https://www.alphavantage.co/documentation/ |
| **API Key** | Required (free tier available) |
| **Free Tier** | 25 requests/day, 5 requests/minute |
| **Cost** | Free tier: $0/month<br>Premium: $50-$500/month |
| **Data** | Stock prices, technical indicators, forex, crypto |

**Our usage:** Fetch 10 stock symbols (AAPL, GOOGL, MSFT, TSLA, NVDA, JPM, GS, BAC, WFC, C) with 100 days history = 10 requests total.

### SEC EDGAR (SEC Filings)

| Property | Value |
|----------|-------|
| **API Docs** | https://www.sec.gov/edgar/sec-api-documentation |
| **API Key** | ❌ Not required |
| **User-Agent** | ✅ Required (company name + email) |
| **Rate Limit** | ~10 requests/second |
| **Cost** | Free (public data) |
| **Data** | 8-K, S-1, 10-K, 10-Q filings, insider trades |

**Our usage:** Fetch 30 recent 8-K filings (M&A announcements, material events).

**Important:** Update the `User-Agent` header in `ingest/fetch_sec_filings.py` with your company name and email for production use.

### Zillow Research Data (Real Estate) - OPTIONAL

| Property | Value |
|----------|-------|
| **Data Portal** | https://www.zillow.com/research/data/ |
| **API Key** | ❌ Not required (CSV downloads) |
| **Rate Limit** | None (manual downloads) |
| **Cost** | Free (public data) |
| **Data** | Home values (ZHVI), sales, inventory, rentals |

**Our usage:** Optional — not currently implemented in the demo, but easy to add.

---

## Resources

- **Snowflake Cortex Docs:** https://docs.snowflake.com/en/user-guide/snowflake-cortex
- **Snowflake ML Docs:** https://docs.snowflake.com/en/user-guide/ml-functions
- **OpenTofu Snowflake Provider:** https://registry.terraform.io/providers/Snowflake-Labs/snowflake
- **Snowpark Python Guide:** https://docs.snowflake.com/en/developer-guide/snowpark/python/index

---

## Support

**Questions or issues?**  
Carson @ Blue Orange Digital  
Slack: `#helm-team`  
Email: carson@blueorange.digital

---

**Status:** ✅ Ready for demo recording  
**Last Updated:** March 16, 2026
