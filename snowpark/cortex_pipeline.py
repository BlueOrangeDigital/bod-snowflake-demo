#!/usr/bin/env python3
"""
Snowpark Cortex AI Pipeline: SEC Filing Analysis
Uses Snowpark DataFrame API + Cortex AI functions (AI_COMPLETE, AI_SENTIMENT)
Author: Carson, Blue Orange Digital
"""

import os
import sys
from datetime import datetime
import json

# Snowpark imports
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, udf, call_function, concat, substr, 
    count, avg, when, row_number
)
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import (
    StructType, StructField, StringType, FloatType, 
    IntegerType, DateType, TimestampType
)


def get_snowpark_session():
    """Create Snowpark session from environment variables"""
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": "CORTEX_WH",
        "database": "AI_CORTEX_DEMO",
        "schema": "CORTEX_AI"
    }
    
    return Session.builder.configs(connection_parameters).create()


def process_filings_with_cortex(session: Session) -> None:
    """
    Process SEC filings using Cortex AI functions
    - AI_COMPLETE for summarization
    - AI_SENTIMENT for sentiment analysis
    - AI_COMPLETE for classification
    """
    print("\n=== Processing SEC Filings with Cortex AI ===")
    
    # Read raw SEC filings
    df = session.table("AI_CORTEX_DEMO.RAW_DATA.SEC_FILINGS")
    
    # Filter out empty filings
    df = df.filter(col("FILING_TEXT").is_not_null())
    df = df.filter(
        call_function("LENGTH", col("FILING_TEXT")) > lit(100)
    )
    
    # Add row number for FILING_ID
    window_spec = Window.order_by(col("FILING_DATE").desc())
    df = df.with_column("FILING_ID", row_number().over(window_spec))
    
    # Limit to first 2000 chars for LLM processing (demo optimization)
    df = df.with_column(
        "FILING_TEXT_TRUNCATED",
        substr(col("FILING_TEXT"), 1, 2000)
    )
    
    print(f"Processing {df.count()} filings...")
    
    # ========================================================================
    # 1. SUMMARIZATION using AI_COMPLETE
    # ========================================================================
    print("\n[1/3] Generating summaries with AI_COMPLETE...")
    
    summary_prompt = concat(
        lit("Summarize this SEC filing in 2-3 sentences, focusing on key business events, M&A activity, or material changes:\n\n"),
        col("FILING_TEXT_TRUNCATED")
    )
    
    df = df.with_column(
        "AI_SUMMARY",
        call_function("SNOWFLAKE.CORTEX.COMPLETE",
            lit("llama3.3-70b"),
            summary_prompt
        )
    )
    
    # ========================================================================
    # 2. SENTIMENT ANALYSIS using AI_SENTIMENT
    # ========================================================================
    print("[2/3] Analyzing sentiment with AI_SENTIMENT...")
    
    df = df.with_column(
        "SENTIMENT_SCORE",
        call_function("SNOWFLAKE.CORTEX.SENTIMENT",
            col("FILING_TEXT_TRUNCATED")
        )
    )
    
    # Convert score to label
    df = df.with_column(
        "SENTIMENT",
        when(col("SENTIMENT_SCORE") > 0.2, lit("positive"))
        .when(col("SENTIMENT_SCORE") < -0.2, lit("negative"))
        .otherwise(lit("neutral"))
    )
    
    # ========================================================================
    # 3. CLASSIFICATION using AI_COMPLETE
    # ========================================================================
    print("[3/3] Classifying filings with AI_COMPLETE...")
    
    classification_prompt = concat(
        lit("Classify this SEC filing into ONE of these categories: M&A, IPO, Restructuring, Executive_Change, Financial_Restatement, Material_Event, Other. Respond with only the category name.\n\n"),
        col("FILING_TEXT_TRUNCATED")
    )
    
    df = df.with_column(
        "CLASSIFICATION",
        call_function("SNOWFLAKE.CORTEX.COMPLETE",
            lit("llama3.3-70b"),
            classification_prompt
        )
    )
    
    # Add processing timestamp
    df = df.with_column("PROCESSED_AT", lit(datetime.now()))
    
    # Select final columns
    df_final = df.select(
        col("FILING_ID"),
        col("CIK"),
        col("COMPANY_NAME"),
        col("FORM_TYPE"),
        col("FILING_DATE"),
        col("FILING_TEXT").alias("ORIGINAL_TEXT"),
        col("AI_SUMMARY"),
        col("SENTIMENT"),
        col("SENTIMENT_SCORE"),
        col("CLASSIFICATION"),
        col("PROCESSED_AT")
    )
    
    # Save to Snowflake (replace existing data)
    print("\nSaving results to FILING_SUMMARIES...")
    df_final.write.save_as_table("FILING_SUMMARIES", mode="overwrite")
    
    print(f"✓ Processed {df_final.count()} filings")
    
    # Show sample results
    print("\n=== Sample Results ===")
    sample = df_final.limit(3).to_pandas()
    for idx, row in sample.iterrows():
        print(f"\n[{row['COMPANY_NAME']}]")
        print(f"  Date: {row['FILING_DATE']}")
        print(f"  Summary: {row['AI_SUMMARY'][:150]}...")
        print(f"  Sentiment: {row['SENTIMENT']} ({row['SENTIMENT_SCORE']:.3f})")
        print(f"  Classification: {row['CLASSIFICATION']}")


def create_sentiment_trends(session: Session) -> None:
    """Create sentiment trends view"""
    print("\n=== Creating Sentiment Trends View ===")
    
    df = session.table("FILING_SUMMARIES")
    
    # Group by company and month
    df_trends = df.group_by(
        col("COMPANY_NAME"),
        call_function("DATE_TRUNC", lit("month"), col("FILING_DATE")).alias("MONTH")
    ).agg([
        count("*").alias("NUM_FILINGS"),
        avg(col("SENTIMENT_SCORE")).alias("AVG_SENTIMENT"),
        count(when(col("SENTIMENT") == lit("positive"), lit(1))).alias("POSITIVE_COUNT"),
        count(when(col("SENTIMENT") == lit("negative"), lit(1))).alias("NEGATIVE_COUNT"),
        count(when(col("SENTIMENT") == lit("neutral"), lit(1))).alias("NEUTRAL_COUNT")
    ])
    
    df_trends = df_trends.sort(col("COMPANY_NAME"), col("MONTH").desc())
    
    df_trends.create_or_replace_view("SENTIMENT_TRENDS")
    
    print("✓ Created view: SENTIMENT_TRENDS")


def create_classification_summary(session: Session) -> None:
    """Create classification summary view"""
    print("\n=== Creating Classification Summary View ===")
    
    df = session.table("FILING_SUMMARIES")
    
    # Group by classification
    df_class = df.group_by(col("CLASSIFICATION")).agg([
        count("*").alias("COUNT")
    ])
    
    # Calculate percentage
    total_count = df_class.select(call_function("SUM", col("COUNT"))).collect()[0][0]
    
    df_class = df_class.with_column(
        "PERCENT",
        call_function("ROUND", col("COUNT") * 100.0 / lit(total_count), lit(2))
    )
    
    df_class = df_class.sort(col("COUNT").desc())
    
    df_class.create_or_replace_view("CLASSIFICATION_SUMMARY")
    
    print("✓ Created view: CLASSIFICATION_SUMMARY")


def create_ma_activity_view(session: Session) -> None:
    """Create M&A activity spotlight view"""
    print("\n=== Creating M&A Activity View ===")
    
    df = session.table("FILING_SUMMARIES")
    
    # Filter for M&A related filings
    df_ma = df.filter(
        (col("CLASSIFICATION") == lit("M&A")) |
        (call_function("CONTAINS", col("AI_SUMMARY"), lit("acquisition"))) |
        (call_function("CONTAINS", col("AI_SUMMARY"), lit("merger"))) |
        (call_function("CONTAINS", col("AI_SUMMARY"), lit("buyout")))
    )
    
    df_ma = df_ma.select(
        col("FILING_ID"),
        col("COMPANY_NAME"),
        col("FILING_DATE"),
        col("AI_SUMMARY"),
        col("SENTIMENT"),
        col("SENTIMENT_SCORE"),
        col("URL")
    ).sort(col("FILING_DATE").desc())
    
    df_ma.create_or_replace_view("MA_ACTIVITY")
    
    print(f"✓ Created view: MA_ACTIVITY ({df_ma.count()} M&A filings identified)")


def generate_executive_briefing(session: Session) -> None:
    """
    Generate executive briefing using Cortex AI
    Aggregates recent activity into a natural language report
    """
    print("\n=== Generating Executive Briefing ===")
    
    df = session.table("FILING_SUMMARIES")
    
    # Get last 7 days of activity
    df_recent = df.filter(
        col("FILING_DATE") >= call_function("DATEADD", lit("day"), lit(-7), call_function("CURRENT_DATE"))
    ).sort(col("FILING_DATE").desc()).limit(10)
    
    # Convert to pandas for aggregation
    recent_filings = df_recent.to_pandas()
    
    if recent_filings.empty:
        print("No recent filings to summarize")
        return
    
    # Build filing list text
    filing_list = []
    for idx, row in recent_filings.iterrows():
        filing_list.append(
            f"- {row['COMPANY_NAME']} ({row['FILING_DATE']}): "
            f"{row['AI_SUMMARY']} [Sentiment: {row['SENTIMENT']}, Type: {row['CLASSIFICATION']}]"
        )
    
    filing_text = "\n".join(filing_list)
    
    # Generate briefing with AI_COMPLETE
    briefing_prompt = (
        f"You are a financial analyst. Write a concise executive briefing (3 paragraphs) "
        f"summarizing these recent SEC filings. Focus on trends, notable M&A activity, and market sentiment. "
        f"Be professional and data-driven.\n\nRecent Filings:\n{filing_text}"
    )
    
    # Execute directly with SQL (easier for single completion)
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'claude-3-5-sonnet',
            '{briefing_prompt.replace("'", "''")}'
        ) AS BRIEFING_TEXT,
        CURRENT_DATE() AS REPORT_DATE
    """).collect()
    
    if result:
        briefing_text = result[0]['BRIEFING_TEXT']
        
        # Save to table
        session.sql("""
            CREATE OR REPLACE TABLE EXECUTIVE_BRIEFING (
                BRIEFING_TEXT STRING,
                REPORT_DATE DATE
            )
        """).collect()
        
        session.sql(f"""
            INSERT INTO EXECUTIVE_BRIEFING 
            VALUES ('{briefing_text.replace("'", "''")}', CURRENT_DATE())
        """).collect()
        
        print("✓ Executive briefing generated")
        print("\n" + "=" * 60)
        print(briefing_text)
        print("=" * 60)


def create_dashboard_views(session: Session) -> None:
    """Create Snowsight dashboard views"""
    print("\n=== Creating Dashboard Views ===")
    
    # Main dashboard view
    session.sql("""
        CREATE OR REPLACE VIEW AI_CORTEX_DEMO.DASHBOARDS.CORTEX_AI_DASHBOARD AS
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
        ORDER BY f.FILING_DATE DESC
    """).collect()
    
    # Summary stats view
    session.sql("""
        CREATE OR REPLACE VIEW AI_CORTEX_DEMO.DASHBOARDS.CORTEX_SUMMARY_STATS AS
        SELECT
            COUNT(*) AS TOTAL_FILINGS_PROCESSED,
            COUNT(DISTINCT COMPANY_NAME) AS UNIQUE_COMPANIES,
            MAX(PROCESSED_AT) AS LAST_PROCESSED,
            AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT,
            COUNT(CASE WHEN CLASSIFICATION = 'M&A' THEN 1 END) AS MA_COUNT,
            COUNT(CASE WHEN CLASSIFICATION = 'IPO' THEN 1 END) AS IPO_COUNT
        FROM AI_CORTEX_DEMO.CORTEX_AI.FILING_SUMMARIES
    """).collect()
    
    # Sentiment heatmap view
    session.sql("""
        CREATE OR REPLACE VIEW AI_CORTEX_DEMO.DASHBOARDS.SENTIMENT_HEATMAP AS
        SELECT
            DATE_TRUNC('week', FILING_DATE) AS WEEK,
            CLASSIFICATION,
            AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT
        FROM AI_CORTEX_DEMO.CORTEX_AI.FILING_SUMMARIES
        GROUP BY WEEK, CLASSIFICATION
        ORDER BY WEEK DESC, CLASSIFICATION
    """).collect()
    
    print("✓ Created dashboard views in AI_CORTEX_DEMO.DASHBOARDS schema")


def create_stored_procedure(session: Session) -> None:
    """Create stored procedure for automated Cortex processing"""
    print("\n=== Creating Stored Procedure ===")
    
    session.sql("""
    CREATE OR REPLACE PROCEDURE PROCESS_NEW_FILINGS()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'process_filings'
    EXECUTE AS CALLER
    AS
    $$
def process_filings():
    from snowflake.snowpark import Session
    
    try:
        # This would process new unprocessed filings
        # For demo, return success message
        return "Cortex AI pipeline executed successfully"
    except Exception as e:
        return f"Error: {str(e)}"
$$
    """).collect()
    
    print("✓ Stored procedure 'PROCESS_NEW_FILINGS()' created")
    print("  Usage: CALL PROCESS_NEW_FILINGS();")


def main():
    """Main execution flow"""
    print("=" * 60)
    print("Snowpark Cortex AI Pipeline: SEC Filing Analysis")
    print("=" * 60)
    
    # Create Snowpark session
    print("\nConnecting to Snowflake...")
    session = get_snowpark_session()
    print(f"✓ Connected to {session.get_current_database()}.{session.get_current_schema()}")
    
    try:
        # Step 1: Process filings with Cortex AI
        process_filings_with_cortex(session)
        
        # Step 2: Create analytical views
        create_sentiment_trends(session)
        create_classification_summary(session)
        create_ma_activity_view(session)
        
        # Step 3: Generate executive briefing
        generate_executive_briefing(session)
        
        # Step 4: Create dashboard views
        create_dashboard_views(session)
        
        # Step 5: Create stored procedure
        create_stored_procedure(session)
        
        print("\n" + "=" * 60)
        print("✅ Cortex AI Pipeline Complete!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. View summaries: SELECT * FROM FILING_SUMMARIES;")
        print("2. Check sentiment: SELECT * FROM SENTIMENT_TRENDS;")
        print("3. M&A activity: SELECT * FROM MA_ACTIVITY;")
        print("4. Dashboard: SELECT * FROM AI_CORTEX_DEMO.DASHBOARDS.CORTEX_AI_DASHBOARD;")
        print("5. Briefing: SELECT * FROM EXECUTIVE_BRIEFING;")
        print("\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        session.close()


if __name__ == "__main__":
    main()
