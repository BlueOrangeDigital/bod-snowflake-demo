# Video Recording Script
## Snowflake AI & Cortex Demo

**Duration:** 3-5 minutes  
**Presenter:** Dean Cirielli  
**Date:** March 2026

---

## Pre-Recording Checklist

- [ ] Run all ingestion scripts (stock prices, SEC filings)
- [ ] Execute ML pipeline SQL (`ml_pipeline.sql`)
- [ ] Execute Cortex AI pipeline SQL (`cortex_pipeline.sql`)
- [ ] Open Snowsight in browser, logged in
- [ ] Prepare screen recording tool (Loom, OBS, QuickTime)
- [ ] Test audio levels

---

## Scene 1: Title Card (0:00 - 0:10)

**[Screen: Title slide or Snowsight home]**

**Script:**

> "Welcome! Today I'm demonstrating **Snowflake AI and Cortex** with a real-world use case: analyzing financial markets using both traditional machine learning and large language models."

---

## Scene 2: Architecture Overview (0:10 - 0:40)

**[Screen: Switch to project README or draw diagram on whiteboard]**

**Script:**

> "Here's what we built:
> 
> We're ingesting **live data** from three free public sources:
> - Stock prices from **Alpha Vantage**
> - Real estate trends from **Zillow**
> - SEC filings from the **EDGAR API**
> 
> All data flows into Snowflake, where we run **two AI pipelines**:
> 1. A **traditional ML pipeline** for stock price prediction using regression
> 2. A **Cortex AI pipeline** using large language models for text summarization and sentiment analysis
>
> Let's see it in action."

---

## Scene 3: Data Ingestion (0:40 - 1:10)

**[Screen: Snowsight SQL worksheet]**

**Script:**

> "First, let's look at the data. Here's our stock price time series—10 symbols, 100 days of history."

**Query 1:**
```sql
USE DATABASE AI_CORTEX_DEMO;
USE SCHEMA RAW_DATA;

SELECT
    SYMBOL,
    DATE,
    CLOSE,
    VOLUME
FROM STOCK_PRICES
WHERE SYMBOL = 'AAPL'
ORDER BY DATE DESC
LIMIT 10;
```

> "And here are the SEC filings we're ingesting—8-K forms with full text content for our LLM analysis."

**Query 2:**
```sql
SELECT
    COMPANY_NAME,
    FORM_TYPE,
    FILING_DATE,
    SUBSTR(FILING_TEXT, 1, 150) AS TEXT_PREVIEW
FROM SEC_FILINGS
ORDER BY FILING_DATE DESC
LIMIT 5;
```

> "Now let's process this data."

---

## Scene 4: Traditional ML Pipeline (1:10 - 2:00)

**[Screen: Snowsight SQL worksheet]**

**Script:**

> "Our **ML pipeline** starts with feature engineering. We calculate moving averages, momentum indicators, and volatility..."

**Query 3:**
```sql
USE SCHEMA ML_MODELS;

SELECT
    SYMBOL,
    DATE,
    TARGET_PRICE AS CLOSE,
    MA_7,
    MA_30,
    MOMENTUM_1D,
    VOLATILITY_7D
FROM STOCK_FEATURES
WHERE SYMBOL = 'AAPL'
ORDER BY DATE DESC
LIMIT 10;
```

> "Using these features, we trained a **linear regression model** on 6 months of historical data. Here are the predictions for the next 7 days, with confidence intervals..."

**Query 4:**
```sql
SELECT
    SYMBOL,
    PREDICTION_DATE,
    PREDICTED_CLOSE,
    CONFIDENCE_INTERVAL_LOW,
    CONFIDENCE_INTERVAL_HIGH
FROM STOCK_PREDICTIONS
WHERE SYMBOL = 'AAPL'
ORDER BY PREDICTION_DATE;
```

> "And here's our model performance across all symbols. We're achieving around **3-5% mean absolute percentage error**—pretty good for a simple linear model."

**Query 5:**
```sql
SELECT
    SYMBOL,
    COUNT(*) AS PREDICTIONS,
    ROUND(AVG(ABSOLUTE_ERROR), 2) AS MAE,
    ROUND(AVG(PERCENT_ERROR), 2) AS MAPE
FROM MODEL_PERFORMANCE
GROUP BY SYMBOL
ORDER BY MAPE;
```

---

## Scene 5: Cortex AI Pipeline (2:00 - 3:30)

**[Screen: Snowsight SQL worksheet]**

**Script:**

> "Now for the exciting part—**Cortex AI**. We're using Snowflake's built-in LLM functions to process unstructured text.
>
> First, we use **AI_COMPLETE** to generate concise summaries of SEC filings..."

**Query 6:**
```sql
USE SCHEMA CORTEX_AI;

SELECT
    COMPANY_NAME,
    FILING_DATE,
    SUBSTR(ORIGINAL_TEXT, 1, 100) AS ORIGINAL_PREVIEW,
    AI_SUMMARY
FROM FILING_SUMMARIES
ORDER BY FILING_DATE DESC
LIMIT 3;
```

> "Next, **AI_SENTIMENT** analyzes the tone—positive, negative, or neutral..."

**Query 7:**
```sql
SELECT
    COMPANY_NAME,
    FILING_DATE,
    SENTIMENT,
    SENTIMENT_SCORE
FROM FILING_SUMMARIES
ORDER BY FILING_DATE DESC
LIMIT 5;
```

> "And we **classify** each filing into categories like M&A, IPO, or Restructuring..."

**Query 8:**
```sql
SELECT
    CLASSIFICATION,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS PERCENT
FROM FILING_SUMMARIES
GROUP BY CLASSIFICATION
ORDER BY COUNT DESC;
```

> "Here's a spotlight on **M&A activity** we detected..."

**Query 9:**
```sql
SELECT
    COMPANY_NAME,
    FILING_DATE,
    AI_SUMMARY,
    SENTIMENT
FROM MA_ACTIVITY
LIMIT 5;
```

> "And finally, we generate an **executive briefing**—a natural language report summarizing all recent activity."

**Query 10:**
```sql
SELECT BRIEFING_TEXT FROM EXECUTIVE_BRIEFING;
```

**[Scroll through the generated briefing text]**

---

## Scene 6: Dashboards (3:30 - 4:00)

**[Screen: Snowsight Dashboards tab]**

**Script:**

> "Everything comes together in **Snowsight dashboards**. Here's our ML prediction view—forecast versus actuals..."

**[Show dashboard with STOCK_PREDICTION_DASHBOARD]**

> "And here's the Cortex AI dashboard with sentiment trends and classification breakdown."

**[Show dashboard with CORTEX_AI_DASHBOARD]**

> "We can see sentiment trends over time, classification distribution, and drill down into individual filings."

---

## Scene 7: Wrap-Up (4:00 - 4:30)

**[Screen: Back to README or closing slide]**

**Script:**

> "So in just **5 minutes**, we demonstrated:
> - **Live data ingestion** from free public APIs
> - **Traditional ML** for time series forecasting with regression
> - **Cortex AI** for LLM-powered summarization, sentiment analysis, and classification
> - All running **entirely within Snowflake**—no external infrastructure.
>
> The entire setup is automated with **OpenTofu**, and the pipelines run on **Snowflake Tasks** for daily updates.
>
> This is just scratching the surface. You could extend this to:
> - Real-time alerting on M&A activity
> - Multi-modal analysis with images and documents
> - Predictive models for market trends
>
> All the code is available in our GitHub repo. Questions? Find me on Slack at **#helm-team**.
>
> Thanks for watching!"

---

## Post-Recording Checklist

- [ ] Review video for clarity and pacing
- [ ] Add captions/subtitles if needed
- [ ] Export in 1080p (recommended)
- [ ] Upload to YouTube/Loom/internal platform
- [ ] Share link with team

---

## Tips for a Great Recording

1. **Speak clearly and confidently** — You're the expert!
2. **Show, don't tell** — Let the queries and results speak for themselves
3. **Use cursor to highlight** — Point to important numbers/text
4. **Keep it concise** — 3-5 minutes means staying focused
5. **Smile!** — Even in a technical demo, energy matters

---

## Backup Queries (If Time Permits)

**Sentiment heatmap:**
```sql
SELECT
    WEEK,
    CLASSIFICATION,
    AVG_SENTIMENT
FROM DASHBOARDS.SENTIMENT_HEATMAP
ORDER BY WEEK DESC
LIMIT 20;
```

**Summary stats:**
```sql
SELECT * FROM DASHBOARDS.ML_SUMMARY_STATS;
SELECT * FROM DASHBOARDS.CORTEX_SUMMARY_STATS;
```

---

**Good luck with the recording!** 🎬
