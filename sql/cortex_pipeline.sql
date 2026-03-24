-- Cortex AI Pipeline: SEC Filing Analysis
-- Uses Snowflake Cortex functions for LLM-powered summarization and classification
-- Author: Carson, Blue Orange Digital

USE DATABASE AI_CORTEX_DEMO;
USE SCHEMA CORTEX_AI;
USE WAREHOUSE CORTEX_WH;

-- ============================================================================
-- STEP 1: Process SEC Filings with Cortex AI
-- ============================================================================

-- Generate AI summaries and classifications for all filings
CREATE OR REPLACE TABLE FILING_SUMMARIES AS
WITH RAW_FILINGS AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY FILING_DATE DESC) AS FILING_ID,
        CIK,
        COMPANY_NAME,
        FORM_TYPE,
        FILING_DATE,
        FILING_TEXT
    FROM AI_CORTEX_DEMO.RAW_DATA.SEC_FILINGS
    WHERE LENGTH(FILING_TEXT) > 100  -- Filter out empty filings
),
WITH_AI AS (
    SELECT
        FILING_ID,
        CIK,
        COMPANY_NAME,
        FORM_TYPE,
        FILING_DATE,
        FILING_TEXT AS ORIGINAL_TEXT,
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-8b',
            CONCAT(
                'Summarize this SEC filing in 2-3 sentences, focusing on key business events, M&A activity, or material changes:\n\n',
                SUBSTR(FILING_TEXT, 1, 2000)
            )
        ) AS AI_SUMMARY,
        SNOWFLAKE.CORTEX.SENTIMENT(SUBSTR(FILING_TEXT, 1, 1000)) AS SENTIMENT_SCORE,
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-8b',
            CONCAT(
                'Classify this SEC filing into ONE of these categories: M&A, IPO, Restructuring, Executive_Change, Financial_Restatement, Material_Event, Other. Respond with only the category name.\n\n',
                SUBSTR(FILING_TEXT, 1, 1000)
            )
        ) AS CLASSIFICATION,
        CURRENT_TIMESTAMP() AS PROCESSED_AT
    FROM RAW_FILINGS
    LIMIT 50
)
SELECT
    FILING_ID,
    CIK,
    COMPANY_NAME,
    FORM_TYPE,
    FILING_DATE,
    ORIGINAL_TEXT,
    AI_SUMMARY,
    SENTIMENT_SCORE,
    CASE
        WHEN SENTIMENT_SCORE > 0.2 THEN 'positive'
        WHEN SENTIMENT_SCORE < -0.2 THEN 'negative'
        ELSE 'neutral'
    END AS SENTIMENT,
    CLASSIFICATION,
    PROCESSED_AT
FROM WITH_AI;

-- ============================================================================
-- STEP 2: Advanced NLP - Entity Extraction
-- ============================================================================

-- Extract key entities (companies, executives, financial amounts) from filings
CREATE OR REPLACE VIEW FILING_ENTITIES AS
SELECT
    FILING_ID,
    COMPANY_NAME,
    FILING_DATE,
    FORM_TYPE,
    
    -- Extract entities using Cortex COMPLETE with structured prompt
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-8b',
        CONCAT(
            'Extract key entities from this SEC filing. Return ONLY a JSON object with these fields: companies (array), executives (array), financial_amounts (array), locations (array). Text:\n\n',
            SUBSTR(ORIGINAL_TEXT, 1, 1500)
        )
    ) AS EXTRACTED_ENTITIES_JSON,
    
    AI_SUMMARY,
    SENTIMENT,
    CLASSIFICATION

FROM FILING_SUMMARIES
WHERE LENGTH(ORIGINAL_TEXT) > 200
LIMIT 20;  -- Demo subset

-- ============================================================================
-- STEP 3: Aggregate Insights
-- ============================================================================

-- Sentiment trends by company
CREATE OR REPLACE VIEW SENTIMENT_TRENDS AS
SELECT
    COMPANY_NAME,
    DATE_TRUNC('month', FILING_DATE) AS MONTH,
    COUNT(*) AS NUM_FILINGS,
    AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT,
    COUNT_IF(SENTIMENT = 'positive') AS POSITIVE_COUNT,
    COUNT_IF(SENTIMENT = 'negative') AS NEGATIVE_COUNT,
    COUNT_IF(SENTIMENT = 'neutral') AS NEUTRAL_COUNT
FROM FILING_SUMMARIES
GROUP BY COMPANY_NAME, MONTH
ORDER BY COMPANY_NAME, MONTH DESC;

-- Classification breakdown
CREATE OR REPLACE VIEW CLASSIFICATION_SUMMARY AS
SELECT
    CLASSIFICATION,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENT,
    LISTAGG(COMPANY_NAME, ', ') WITHIN GROUP (ORDER BY COMPANY_NAME) AS COMPANIES
FROM FILING_SUMMARIES
GROUP BY CLASSIFICATION
ORDER BY COUNT DESC;

-- M&A Activity Spotlight
CREATE OR REPLACE VIEW MA_ACTIVITY AS
SELECT
    FILING_ID,
    COMPANY_NAME,
    FILING_DATE,
    AI_SUMMARY,
    SENTIMENT,
    SENTIMENT_SCORE
FROM FILING_SUMMARIES
WHERE CLASSIFICATION = 'M&A'
   OR AI_SUMMARY ILIKE '%acquisition%'
   OR AI_SUMMARY ILIKE '%merger%'
   OR AI_SUMMARY ILIKE '%buyout%'
ORDER BY FILING_DATE DESC;

-- ============================================================================
-- STEP 4: Generative AI - Create Executive Briefing
-- ============================================================================

-- Generate an executive summary report using Cortex AI
CREATE OR REPLACE VIEW EXECUTIVE_BRIEFING AS
WITH RECENT_ACTIVITY AS (
    SELECT
        COMPANY_NAME,
        FORM_TYPE,
        FILING_DATE,
        AI_SUMMARY,
        SENTIMENT,
        CLASSIFICATION
    FROM FILING_SUMMARIES
    WHERE FILING_DATE >= DATEADD(day, -7, CURRENT_DATE())
    ORDER BY FILING_DATE DESC
    LIMIT 10
),
CONCATENATED AS (
    SELECT LISTAGG(
        CONCAT(
            '- ', COMPANY_NAME, ' (', FILING_DATE, '): ',
            AI_SUMMARY, ' [Sentiment: ', SENTIMENT, ', Type: ', CLASSIFICATION, ']'
        ),
        '\n'
    ) AS FILING_LIST
    FROM RECENT_ACTIVITY
)
SELECT
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-8b',
        CONCAT(
            'You are a financial analyst. Write a concise executive briefing (3 paragraphs) summarizing these recent SEC filings. Focus on trends, notable M&A activity, and market sentiment. Be professional and data-driven.\n\nRecent Filings:\n',
            FILING_LIST
        )
    ) AS BRIEFING_TEXT,
    CURRENT_DATE() AS REPORT_DATE
FROM CONCATENATED;

-- ============================================================================
-- STEP 5: Dashboard Views (for Snowsight)
-- ============================================================================

USE SCHEMA DASHBOARDS;

-- Main Cortex AI dashboard view
CREATE OR REPLACE VIEW CORTEX_AI_DASHBOARD AS
SELECT
    f.FILING_ID,
    f.COMPANY_NAME,
    f.FORM_TYPE,
    f.FILING_DATE,
    f.AI_SUMMARY,
    f.SENTIMENT,
    f.SENTIMENT_SCORE,
    f.CLASSIFICATION,
    CASE
        WHEN f.CLASSIFICATION = 'M&A' THEN '🤝'
        WHEN f.CLASSIFICATION = 'IPO' THEN '🚀'
        WHEN f.CLASSIFICATION = 'Restructuring' THEN '🏗️'
        WHEN f.SENTIMENT = 'positive' THEN '📈'
        WHEN f.SENTIMENT = 'negative' THEN '📉'
        ELSE '📄'
    END AS ICON,
    f.PROCESSED_AT
FROM AI_CORTEX_DEMO.CORTEX_AI.FILING_SUMMARIES f
ORDER BY f.FILING_DATE DESC;

-- Summary statistics for dashboard
CREATE OR REPLACE VIEW CORTEX_SUMMARY_STATS AS
SELECT
    COUNT(*) AS TOTAL_FILINGS_PROCESSED,
    COUNT(DISTINCT COMPANY_NAME) AS UNIQUE_COMPANIES,
    MAX(PROCESSED_AT) AS LAST_PROCESSED,
    AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT,
    COUNT_IF(CLASSIFICATION = 'M&A') AS MA_COUNT,
    COUNT_IF(CLASSIFICATION = 'IPO') AS IPO_COUNT
FROM AI_CORTEX_DEMO.CORTEX_AI.FILING_SUMMARIES;

-- Sentiment heatmap data
CREATE OR REPLACE VIEW SENTIMENT_HEATMAP AS
SELECT
    DATE_TRUNC('week', FILING_DATE) AS WEEK,
    CLASSIFICATION,
    AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT
FROM AI_CORTEX_DEMO.CORTEX_AI.FILING_SUMMARIES
GROUP BY WEEK, CLASSIFICATION
ORDER BY WEEK DESC, CLASSIFICATION;

-- ============================================================================
-- STEP 6: Automated Task (Optional - for live updates)
-- ============================================================================

-- Create a task to process new filings with Cortex AI
CREATE OR REPLACE TASK PROCESS_NEW_FILINGS
    WAREHOUSE = CORTEX_WH
    SCHEDULE = 'USING CRON 0 */6 * * * America/New_York'  -- Every 6 hours
AS
    SELECT 'Task placeholder - replace with Snowpark stored procedure call';

-- Note: Task is created but not started. To start:
-- ALTER TASK PROCESS_NEW_FILINGS RESUME;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check processed summaries
SELECT * FROM AI_CORTEX_DEMO.CORTEX_AI.FILING_SUMMARIES ORDER BY FILING_DATE DESC LIMIT 10;

-- Check sentiment trends
SELECT * FROM AI_CORTEX_DEMO.CORTEX_AI.SENTIMENT_TRENDS ORDER BY MONTH DESC, COMPANY_NAME LIMIT 20;

-- Check classification breakdown
SELECT * FROM AI_CORTEX_DEMO.CORTEX_AI.CLASSIFICATION_SUMMARY;

-- Check M&A activity
SELECT * FROM AI_CORTEX_DEMO.CORTEX_AI.MA_ACTIVITY LIMIT 10;

-- Check executive briefing
SELECT * FROM AI_CORTEX_DEMO.CORTEX_AI.EXECUTIVE_BRIEFING;

-- Check dashboard view
SELECT * FROM AI_CORTEX_DEMO.DASHBOARDS.CORTEX_AI_DASHBOARD LIMIT 20;

-- Check summary stats
SELECT * FROM AI_CORTEX_DEMO.DASHBOARDS.CORTEX_SUMMARY_STATS;
