# Snowpark Python Guide

**Complete guide to running AI pipelines with Snowpark Python**

---

## Overview

This demo provides **two approaches** for AI/ML pipelines:

| Approach | Best For | Complexity | Production-Ready |
|----------|----------|------------|------------------|
| **SQL** (`sql/`) | Quick demos, prototyping, simple pipelines | Low ⭐ | ✅ Yes (limited) |
| **Snowpark Python** (`snowpark/`) | Production ML, custom models, advanced analytics | Medium ⭐⭐⭐ | ✅✅✅ Yes (full) |

---

## SQL Approach (`sql/` directory)

**Files:**
- `sql/ml_pipeline.sql` — Traditional ML with built-in Snowflake functions
- `sql/cortex_pipeline.sql` — Cortex AI with SQL functions

**Pros:**
✅ Simple — runs directly in Snowsight  
✅ No Python setup required  
✅ Great for quick demos  

**Cons:**
❌ Limited to Snowflake's built-in ML capabilities  
❌ No custom models or feature engineering  
❌ Basic regression only (no XGBoost, neural nets, etc.)  

**Use when:**
- Doing a quick 5-minute demo
- Audience is SQL-focused (analysts, BI teams)
- Simple use cases (basic forecasting, sentiment analysis)

---

## Snowpark Python Approach (`snowpark/` directory)

**Files:**
- `snowpark/ml_pipeline.py` — XGBoost model with advanced feature engineering
- `snowpark/cortex_pipeline.py` — Cortex AI with Python UDFs and DataFrame API

**Pros:**
✅ **Full Python ML ecosystem** (scikit-learn, XGBoost, PyTorch, etc.)  
✅ **Custom models** and advanced feature engineering  
✅ **Better accuracy** — XGBoost vs simple linear regression  
✅ **Production-ready** — stored procedures, model registry, versioning  
✅ **Code runs in Snowflake** — no data movement  

**Cons:**
❌ More setup (Python, dependencies)  
❌ Slightly more complex code  

**Use when:**
- Building production ML systems
- Need custom models or advanced algorithms
- Audience is technical (data scientists, ML engineers)
- Want to showcase Snowflake's full data science platform

---

## Quick Start: Snowpark Python

### Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

**Includes:**
- `snowflake-snowpark-python` — Snowpark DataFrame API
- `scikit-learn` — Feature engineering, preprocessing
- `xgboost` — Gradient boosting models
- `pandas`, `numpy` — Data manipulation

### Step 2: Set Environment Variables

```bash
export SNOWFLAKE_ACCOUNT="<your-account>"
export SNOWFLAKE_USER="<your-username>"
export SNOWFLAKE_PASSWORD="<your-password>"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"
```

### Step 3: Run ML Pipeline

```bash
python snowpark/ml_pipeline.py
```

**What it does:**
1. Connects to Snowflake via Snowpark
2. Creates features using Snowpark DataFrame API (in-database)
3. Trains XGBoost model with hyperparameter tuning (GridSearchCV)
4. Generates predictions for next 7 days
5. Stores results in `STOCK_PREDICTIONS` table
6. Creates stored procedure for automation

**Output:**
```
=== Model Performance ===
MAE:  $2.34
RMSE: $3.12
MAPE: 2.87%
R²:   0.9412
```

### Step 4: Run Cortex Pipeline

```bash
python snowpark/cortex_pipeline.py
```

**What it does:**
1. Reads SEC filings from Snowflake
2. Processes with Cortex AI functions via Snowpark:
   - `AI_COMPLETE()` for summarization
   - `AI_SENTIMENT()` for sentiment analysis
   - `AI_COMPLETE()` for classification
3. Creates analytical views (sentiment trends, M&A activity)
4. Generates executive briefing report
5. Creates dashboard views for Snowsight

**Output:**
```
✓ Processed 30 filings
✓ Created view: SENTIMENT_TRENDS
✓ Created view: MA_ACTIVITY (8 M&A filings identified)
✓ Executive briefing generated
```

---

## Comparison: SQL vs Snowpark

### ML Pipeline

| Feature | SQL (`ml_pipeline.sql`) | Snowpark (`ml_pipeline.py`) |
|---------|-------------------------|----------------------------|
| **Model** | Linear regression (built-in) | XGBoost with hyperparameter tuning |
| **Feature Engineering** | Basic window functions | Advanced transformations + custom logic |
| **Accuracy (MAPE)** | ~5-7% | ~2-3% (better) |
| **Training** | Snowflake ML FORECAST | GridSearchCV + cross-validation |
| **Deployment** | SQL stored procedure | Python stored procedure + model registry |
| **Extensibility** | Limited to Snowflake functions | Full Python ML ecosystem |

**Recommendation:** Use Snowpark for production ML workloads.

### Cortex AI Pipeline

| Feature | SQL (`cortex_pipeline.sql`) | Snowpark (`cortex_pipeline.py`) |
|---------|-----------------------------|---------------------------------|
| **Summarization** | `AI_COMPLETE()` SQL function | `call_function()` via Snowpark |
| **Sentiment** | `AI_SENTIMENT()` SQL function | `call_function()` via Snowpark |
| **Classification** | SQL string matching | Python logic + Cortex AI |
| **Flexibility** | Fixed SQL queries | Dynamic Python logic |
| **Error Handling** | Basic SQL error handling | Robust Python exception handling |
| **Testing** | Manual SQL testing | Unit tests + integration tests |

**Recommendation:** Use SQL for simple Cortex demos; Snowpark for production pipelines with complex logic.

---

## Advanced: Snowpark ML (Optional)

For **production ML** at scale, use **Snowflake ML** (requires `snowflake-ml-python`):

### Model Registry

```python
from snowflake.ml.registry import Registry

# Create registry
registry = Registry(session)

# Log model
model_ref = registry.log_model(
    model=xgb_model,
    model_name="stock_price_predictor",
    version_name="v1",
    conda_dependencies=["xgboost", "scikit-learn"]
)

# Deploy as UDF
model_ref.run(X_test, function_name="PREDICT_STOCK_PRICE")
```

### Feature Store

```python
from snowflake.ml.feature_store import (
    FeatureStore,
    FeatureView,
    Entity
)

# Create feature store
fs = FeatureStore(session, database="AI_CORTEX_DEMO", schema="FEATURE_STORE")

# Define entity (stock symbol)
stock_entity = Entity(name="STOCK", join_keys=["SYMBOL"])

# Create feature view
fv = FeatureView(
    name="stock_features",
    entities=[stock_entity],
    feature_df=df_features,
    refresh_freq="1 day"
)

fs.register_feature_view(fv)
```

---

## Production Deployment

### 1. Automated Pipelines (Snowflake Tasks)

**ML Pipeline (daily retraining):**
```sql
CREATE OR REPLACE TASK DAILY_ML_RETRAINING
    WAREHOUSE = ML_WH
    SCHEDULE = 'USING CRON 0 2 * * * America/New_York'  -- 2 AM ET
AS
    CALL RUN_ML_PIPELINE();
```

**Cortex Pipeline (every 6 hours):**
```sql
CREATE OR REPLACE TASK PROCESS_NEW_FILINGS_TASK
    WAREHOUSE = CORTEX_WH
    SCHEDULE = 'USING CRON 0 */6 * * * America/New_York'
AS
    CALL PROCESS_NEW_FILINGS();
```

**Start tasks:**
```sql
ALTER TASK DAILY_ML_RETRAINING RESUME;
ALTER TASK PROCESS_NEW_FILINGS_TASK RESUME;
```

### 2. Monitoring & Alerting

**Query task history:**
```sql
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME IN ('DAILY_ML_RETRAINING', 'PROCESS_NEW_FILINGS_TASK')
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
```

**Check Cortex usage:**
```sql
SELECT
    DATE_TRUNC('day', START_TIME) AS DAY,
    COUNT(*) AS REQUESTS,
    SUM(CREDITS_USED) AS TOTAL_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
WHERE FUNCTION_NAME IN ('COMPLETE', 'SENTIMENT')
GROUP BY DAY
ORDER BY DAY DESC;
```

### 3. CI/CD Integration

**Example GitHub Actions workflow:**

```yaml
name: Deploy ML Pipeline

on:
  push:
    branches: [main]
    paths:
      - 'snowpark/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: pip install -r requirements.txt
      
      - name: Run tests
        run: pytest tests/
      
      - name: Deploy to Snowflake
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        run: |
          python snowpark/ml_pipeline.py
          python snowpark/cortex_pipeline.py
```

---

## Performance Optimization

### 1. Warehouse Sizing

| Workload | Warehouse Size | Cost/Hour | Recommendation |
|----------|----------------|-----------|----------------|
| Data ingestion | XSMALL | $2 | Small batches |
| Feature engineering | SMALL-MEDIUM | $4-$8 | Medium datasets |
| XGBoost training | MEDIUM-LARGE | $8-$16 | Large training sets |
| Cortex AI | SMALL | $4 | LLM inference |

**Auto-suspend:** Set to 60-300 seconds to minimize costs.

### 2. Cortex AI Optimization

**Batch processing** (faster + cheaper):
```python
# Process filings in batches of 10
batch_size = 10
for i in range(0, len(filings), batch_size):
    batch = filings[i:i+batch_size]
    # Process batch with Cortex
```

**Model selection:**
- `llama3.3-70b` — Good balance (speed + quality)
- `mistral-large2` — Faster, cheaper
- `claude-3-5-sonnet` — Best quality, higher cost

### 3. Caching

**Cache feature computations:**
```python
# Materialize features as table (not view)
df_features.write.save_as_table("STOCK_FEATURES_CACHED", mode="overwrite")
```

**Cache Cortex results:**
```sql
-- Avoid re-processing same filings
CREATE TABLE IF NOT EXISTS CORTEX_CACHE (
    FILING_HASH STRING,
    AI_SUMMARY STRING,
    SENTIMENT_SCORE FLOAT,
    CACHED_AT TIMESTAMP
);
```

---

## Troubleshooting

### Issue: `snowflake.snowpark` not found

**Solution:**
```bash
pip install --upgrade snowflake-snowpark-python
```

### Issue: XGBoost import error

**Solution:**
```bash
pip install xgboost==2.0.3
```

### Issue: `CORTEX AI functions not available`

**Solution:**
```sql
GRANT USE AI FUNCTIONS ON ACCOUNT TO ROLE ACCOUNTADMIN;
```

### Issue: Snowpark session timeout

**Solution:** Increase session timeout:
```python
session._conn._conn.login_timeout = 600  # 10 minutes
```

### Issue: Out of memory during training

**Solution:** Use larger warehouse or reduce dataset size:
```python
# Sample dataset
df_sample = df.sample(frac=0.5)  # Use 50% of data
```

---

## Next Steps

1. **Run both approaches** — Compare SQL vs Snowpark results
2. **Extend feature engineering** — Add more technical indicators
3. **Try different models** — Random Forest, LSTM, transformers
4. **Set up monitoring** — Track model drift, performance degradation
5. **Deploy to production** — Automate with Snowflake Tasks
6. **Build Streamlit app** — Interactive UI for predictions

---

## Resources

- **Snowpark Python Docs:** https://docs.snowflake.com/en/developer-guide/snowpark/python/index
- **Snowpark ML Docs:** https://docs.snowflake.com/en/developer-guide/snowflake-ml/index
- **Cortex AI Docs:** https://docs.snowflake.com/en/user-guide/snowflake-cortex
- **XGBoost Docs:** https://xgboost.readthedocs.io/

---

**Questions?**  
Carson @ Blue Orange Digital  
Slack: `#helm-team`
