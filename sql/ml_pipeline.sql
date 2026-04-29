-- ML Pipeline: Stock Price Prediction using Snowflake ML
-- Traditional regression model to forecast next-day closing prices
-- Author: Carson, Blue Orange Digital

USE ROLE ACCOUNTADMIN;
USE DATABASE AI_CORTEX_DEMO;
USE SCHEMA ML_MODELS;
USE WAREHOUSE ML_WH;

-- ============================================================================
-- STEP 1: Feature Engineering
-- ============================================================================

-- Create features for time series prediction
-- Features: moving averages, price momentum, volatility
CREATE OR REPLACE VIEW STOCK_FEATURES AS
SELECT
    SYMBOL,
    DATE,
    CLOSE AS TARGET_PRICE,
    
    -- Moving averages
    AVG(CLOSE) OVER (
        PARTITION BY SYMBOL
        ORDER BY DATE
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS MA_7,
    
    AVG(CLOSE) OVER (
        PARTITION BY SYMBOL
        ORDER BY DATE
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
    ) AS MA_30,
    
    -- Price momentum (% change)
    (CLOSE - LAG(CLOSE, 1) OVER (PARTITION BY SYMBOL ORDER BY DATE)) / 
        LAG(CLOSE, 1) OVER (PARTITION BY SYMBOL ORDER BY DATE) * 100 AS MOMENTUM_1D,
    
    (CLOSE - LAG(CLOSE, 7) OVER (PARTITION BY SYMBOL ORDER BY DATE)) /
        LAG(CLOSE, 7) OVER (PARTITION BY SYMBOL ORDER BY DATE) * 100 AS MOMENTUM_7D,
    
    -- Volatility (standard deviation of returns)
    STDDEV(CLOSE) OVER (
        PARTITION BY SYMBOL
        ORDER BY DATE
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS VOLATILITY_7D,
    
    -- Volume momentum
    (VOLUME - AVG(VOLUME) OVER (
        PARTITION BY SYMBOL
        ORDER BY DATE
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )) / AVG(VOLUME) OVER (
        PARTITION BY SYMBOL
        ORDER BY DATE
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS VOLUME_MOMENTUM,
    
    -- Day of week (cyclical patterns)
    DAYOFWEEK(DATE) AS DAY_OF_WEEK,
    
    -- Previous day close (lag feature)
    LAG(CLOSE, 1) OVER (PARTITION BY SYMBOL ORDER BY DATE) AS PREV_CLOSE

FROM AI_CORTEX_DEMO.RAW_DATA.STOCK_PRICES
WHERE DATE >= DATEADD(month, -6, CURRENT_DATE())  -- Last 6 months for training
ORDER BY SYMBOL, DATE;

-- ============================================================================
-- STEP 2: Train ML Model (Linear Regression)
-- ============================================================================

-- Create and train a linear regression model
-- This uses Snowflake's built-in ML capabilities
-- Train on raw prices only (no exogenous features) so FORECAST can run without future feature data
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST STOCK_PRICE_MODEL (
    INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT SYMBOL, DATE, CLOSE AS TARGET_PRICE FROM AI_CORTEX_DEMO.RAW_DATA.STOCK_PRICES'),
    SERIES_COLNAME => 'SYMBOL',
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'TARGET_PRICE'
);

-- Note: Snowflake ML FORECAST is simplified for demo
-- In production, you'd use:
-- - SNOWPARK ML for more complex models
-- - External ML frameworks (scikit-learn, XGBoost)
-- - Stored procedures with Python UDFs

-- ============================================================================
-- STEP 3: Generate Predictions
-- ============================================================================

-- Predict next 7 days of stock prices
CREATE OR REPLACE TABLE STOCK_PREDICTIONS AS
WITH LATEST_DATA AS (
    SELECT
        SYMBOL,
        MAX(DATE) AS LAST_DATE,
        AVG(CLOSE) AS AVG_PRICE,
        STDDEV(CLOSE) AS PRICE_STDDEV
    FROM AI_CORTEX_DEMO.RAW_DATA.STOCK_PRICES
    WHERE DATE >= DATEADD(month, -1, CURRENT_DATE())
    GROUP BY SYMBOL
),
FORECAST AS (
    SELECT
        ts.SERIES AS SYMBOL,
        ts.TS AS PREDICTION_DATE,
        ts.FORECAST AS PREDICTED_CLOSE,
        ts.LOWER_BOUND AS CONFIDENCE_INTERVAL_LOW,
        ts.UPPER_BOUND AS CONFIDENCE_INTERVAL_HIGH
    FROM
        TABLE(
            STOCK_PRICE_MODEL!FORECAST(
                FORECASTING_PERIODS => 7
            )
        ) AS ts
)
SELECT
    f.SYMBOL,
    f.PREDICTION_DATE,
    f.PREDICTED_CLOSE,
    f.CONFIDENCE_INTERVAL_LOW,
    f.CONFIDENCE_INTERVAL_HIGH,
    'v1.0' AS MODEL_VERSION,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM FORECAST f
ORDER BY f.SYMBOL, f.PREDICTION_DATE;

-- ============================================================================
-- STEP 4: Model Evaluation Metrics
-- ============================================================================

-- Compare predictions vs actuals (backtesting)
CREATE OR REPLACE VIEW MODEL_PERFORMANCE AS
WITH ACTUALS AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE AS ACTUAL_PRICE
    FROM AI_CORTEX_DEMO.RAW_DATA.STOCK_PRICES
    WHERE DATE >= DATEADD(day, -30, CURRENT_DATE())
),
PREDICTIONS AS (
    SELECT
        SYMBOL,
        PREDICTION_DATE,
        PREDICTED_CLOSE
    FROM STOCK_PREDICTIONS
)
SELECT
    a.SYMBOL,
    a.DATE,
    a.ACTUAL_PRICE,
    p.PREDICTED_CLOSE,
    ABS(a.ACTUAL_PRICE - p.PREDICTED_CLOSE) AS ABSOLUTE_ERROR,
    ABS(a.ACTUAL_PRICE - p.PREDICTED_CLOSE) / a.ACTUAL_PRICE * 100 AS PERCENT_ERROR
FROM ACTUALS a
LEFT JOIN PREDICTIONS p
    ON a.SYMBOL = p.SYMBOL
    AND a.DATE = p.PREDICTION_DATE
WHERE p.PREDICTED_CLOSE IS NOT NULL
ORDER BY a.SYMBOL, a.DATE;

-- Calculate aggregate metrics
SELECT
    SYMBOL,
    COUNT(*) AS NUM_PREDICTIONS,
    AVG(ABSOLUTE_ERROR) AS MAE,  -- Mean Absolute Error
    SQRT(AVG(POWER(ABSOLUTE_ERROR, 2))) AS RMSE,  -- Root Mean Squared Error
    AVG(PERCENT_ERROR) AS MAPE  -- Mean Absolute Percentage Error
FROM MODEL_PERFORMANCE
GROUP BY SYMBOL
ORDER BY MAPE;

-- ============================================================================
-- STEP 5: Dashboard View (for Snowsight)
-- ============================================================================

USE SCHEMA DASHBOARDS;

CREATE OR REPLACE VIEW STOCK_PREDICTION_DASHBOARD AS
WITH RECENT_PRICES AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE AS PRICE,
        'actual' AS TYPE
    FROM AI_CORTEX_DEMO.RAW_DATA.STOCK_PRICES
    WHERE DATE >= DATEADD(day, -30, CURRENT_DATE())
),
PREDICTIONS AS (
    SELECT
        SYMBOL,
        PREDICTION_DATE AS DATE,
        PREDICTED_CLOSE AS PRICE,
        'predicted' AS TYPE
    FROM AI_CORTEX_DEMO.ML_MODELS.STOCK_PREDICTIONS
)
SELECT * FROM RECENT_PRICES
UNION ALL
SELECT * FROM PREDICTIONS
ORDER BY SYMBOL, DATE;

-- Summary statistics for the dashboard
CREATE OR REPLACE VIEW ML_SUMMARY_STATS AS
SELECT
    COUNT(DISTINCT SYMBOL) AS NUM_STOCKS,
    COUNT(*) AS NUM_PREDICTIONS,
    MAX(CREATED_AT) AS LAST_RUN,
    AVG(PREDICTED_CLOSE) AS AVG_PREDICTED_PRICE
FROM AI_CORTEX_DEMO.ML_MODELS.STOCK_PREDICTIONS;

-- ============================================================================
-- STEP 6: Automated Task (Optional - for live updates)
-- ============================================================================

-- Create a task to refresh predictions daily
CREATE OR REPLACE TASK REFRESH_STOCK_PREDICTIONS
    WAREHOUSE = ML_WH
    SCHEDULE = 'USING CRON 0 9 * * * America/New_York'  -- 9 AM ET daily
AS
    SELECT 'Task placeholder - replace with prediction stored procedure call';

-- Note: Task is created but not started. To start:
-- ALTER TASK REFRESH_STOCK_PREDICTIONS RESUME;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check feature data
SELECT * FROM AI_CORTEX_DEMO.ML_MODELS.STOCK_FEATURES WHERE SYMBOL = 'AAPL' ORDER BY DATE DESC LIMIT 10;

-- Check predictions
SELECT * FROM AI_CORTEX_DEMO.ML_MODELS.STOCK_PREDICTIONS ORDER BY SYMBOL, PREDICTION_DATE LIMIT 20;

-- Check performance metrics
SELECT * FROM AI_CORTEX_DEMO.ML_MODELS.MODEL_PERFORMANCE ORDER BY SYMBOL, DATE DESC LIMIT 20;

-- Check dashboard view
SELECT * FROM AI_CORTEX_DEMO.DASHBOARDS.STOCK_PREDICTION_DASHBOARD WHERE SYMBOL = 'AAPL' ORDER BY DATE DESC LIMIT 30;
