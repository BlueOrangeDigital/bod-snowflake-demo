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
        FILING_TEXT,
        URL
    FROM AI_CORTEX_DEMO.RAW_DATA.SEC_FILINGS
    WHERE LENGTH(FILING_TEXT) > 100  -- Filter out empty filings
)
SELECT
    FILING_ID,
    CIK,
    COMPANY_NAME,
    FORM_TYPE,
    FILING_DATE,
    FILING_TEXT AS ORIGINAL_TEXT,
    
    -- Cortex AI_COMPLETE: Generate concise summary
    SNOWFLAKE.CORTEX.AI_COMPLETE(
        'llama3.3-70b',
        CONCAT(
            'Summarize this SEC filing in 2-3 sentences, focusing on key business events, M&A activity, or material changes:\n\n',
            SUBSTR(FILING_TEXT, 1, 2000)  -- Limit input to 2000 chars for demo
        )
    ) AS AI_SUMMARY,
    
    -- Cortex AI_SENTIMENT: Analyze sentiment
    SNOWFLAKE.CORTEX.AI_SENTIMENT(SUBSTR(FILING_TEXT, 1, 1000)) AS SENTIMENT_SCORE,
    
    -- Cortex AI_CLASSIFY: Classify filing type/topic
    SNOWFLAKE.CORTEX.AI_COMPLETE(
        'llama3.3-70b',
        CONCAT(
            'Classify this SEC filing into ONE of these categories: M&A, IPO, Restructuring, Executive_Change, Financial_Restatement, Material_Event, Other. Respond with only the category name.\n\n',
            SUBSTR(FILING_TEXT, 1, 1000)
        )
    ) AS CLASSIFICATION,
    
    CURRENT_TIMESTAMP() AS PROCESSED_AT

FROM RAW_FILINGS
LIMIT 50;  -- Process 50 most recent filings for demo

-- Add sentiment label based on score
UPDATE FILING_SUMMARIES
SET SENTIMENT = CASE
    WHEN SENTIMENT_SCORE > 0.2 THEN 'positive'
    WHEN SENTIMENT_SCORE < -0.2 THEN 'negative'
    ELSE 'neutral'
END;

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
    
    -- Extract entities using Cortex AI_COMPLETE with structured prompt
    SNOWFLAKE.CORTEX.AI_COMPLETE(
        'mistral-large2',
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
    LIST(COMPANY_NAME) AS COMPANIES  -- Aggregate company names
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
    SENTIMENT_SCORE,
    URL
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
    SNOWFLAKE.CORTEX.AI_COMPLETE(
        'claude-3-5-sonnet',
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
    f.URL,
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
BEGIN
    -- Insert new summaries for unprocessed filings
    INSERT INTO AI_CORTEX_DEMO.CORTEX_AI.FILING_SUMMARIES
    SELECT
        ROW_NUMBER() OVER (ORDER BY FILING_DATE DESC) + 
            (SELECT COALESCE(MAX(FILING_ID), 0) FROM AI_CORTEX_DEMO.CORTEX_AI.FILING_SUMMARIES) AS FILING_ID,
        CIK,
        COMPANY_NAME,
        FORM_TYPE,
        FILING_DATE,
        FILING_TEXT AS ORIGINAL_TEXT,
        
        SNOWFLAKE.CORTEX.AI_COMPLETE(
            'llama3.3-70b',
            CONCAT(
                'Summarize this SEC filing in 2-3 sentences:\n\n',
                SUBSTR(FILING_TEXT, 1, 2000)
            )
        ) AS AI_SUMMARY,
        
        SNOWFLAKE.CORTEX.AI_SENTIMENT(SUBSTR(FILING_TEXT, 1, 1000)) AS SENTIMENT_SCORE,
        
        CASE
            WHEN SNOWFLAKE.CORTEX.AI_SENTIMENT(SUBSTR(FILING_TEXT, 1, 1000)) > 0.2 THEN 'positive'
            WHEN SNOWFLAKE.CORTEX.AI_SENTIMENT(SUBSTR(FILING_TEXT, 1, 1000)) < -0.2 THEN 'negative'
            ELSE 'neutral'
        END AS SENTIMENT,
        
        SNOWFLAKE.CORTEX.AI_COMPLETE(
            'llama3.3-70b',
            CONCAT(
                'Classify as: M&A, IPO, Restructuring, Executive_Change, Financial_Restatement, Material_Event, or Other.\n\n',
                SUBSTR(FILING_TEXT, 1, 1000)
            )
        ) AS CLASSIFICATION,
        
        CURRENT_TIMESTAMP() AS PROCESSED_AT
        
    FROM AI_CORTEX_DEMO.RAW_DATA.SEC_FILINGS sf
    WHERE NOT EXISTS (
        SELECT 1 FROM AI_CORTEX_DEMO.CORTEX_AI.FILING_SUMMARIES fs
        WHERE fs.CIK = sf.CIK
          AND fs.FILING_DATE = sf.FILING_DATE
          AND fs.FORM_TYPE = sf.FORM_TYPE
    )
    LIMIT 10;  -- Process 10 new filings per run
END;

-- Note: Task is created but not started. To start:
-- ALTER TASK PROCESS_NEW_FILINGS RESUME;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check processed summaries
SELECT * FROM FILING_SUMMARIES ORDER BY FILING_DATE DESC LIMIT 10;

-- Check sentiment trends
SELECT * FROM SENTIMENT_TRENDS ORDER BY MONTH DESC, COMPANY_NAME LIMIT 20;

-- Check classification breakdown
SELECT * FROM CLASSIFICATION_SUMMARY;

-- Check M&A activity
SELECT * FROM MA_ACTIVITY LIMIT 10;

-- Check executive briefing
SELECT * FROM EXECUTIVE_BRIEFING;

-- Check dashboard view
SELECT * FROM AI_CORTEX_DEMO.DASHBOARDS.CORTEX_AI_DASHBOARD LIMIT 20;

-- Check summary stats
SELECT * FROM AI_CORTEX_DEMO.DASHBOARDS.CORTEX_SUMMARY_STATS;
