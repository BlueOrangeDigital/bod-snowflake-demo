# Snowflake AI & Cortex Demo - Infrastructure
# OpenTofu/Terraform configuration
# Author: Carson, Blue Orange Digital
# Date: March 16, 2026

terraform {
  required_version = ">= 1.5"
  
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.100"
    }
  }
}

# Provider configuration
# Authenticate via environment variables:
# export SNOWFLAKE_ORGANIZATION_NAME="<your-org-name>"
# export SNOWFLAKE_ACCOUNT_NAME="<your-account-name>"
# export SNOWFLAKE_USER="<your-username>"
# export SNOWFLAKE_PASSWORD="<your-password>"
# export SNOWFLAKE_ROLE="ACCOUNTADMIN"

provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = var.snowflake_role
}

variable "snowflake_organization_name" {
  description = "Snowflake organization name"
}

variable "snowflake_account_name" {
  description = "Snowflake account name"
}

variable "snowflake_user" {
  description = "Snowflake username"
}

variable "snowflake_password" {
  description = "Snowflake password"
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role"
  default     = "ACCOUNTADMIN"
}

# Variables
variable "database_name" {
  description = "Main database for demo"
  default     = "AI_CORTEX_DEMO"
}

variable "warehouse_size" {
  description = "Warehouse size for ML compute"
  default     = "MEDIUM"
}

variable "ingestion_warehouse_size" {
  description = "Warehouse size for ingestion"
  default     = "XSMALL"
}

variable "cortex_warehouse_size" {
  description = "Warehouse size for Cortex AI"
  default     = "SMALL"
}


# Database
resource "snowflake_database" "demo" {
  name    = var.database_name
  comment = "AI and Cortex demo database"
}

# Schemas
resource "snowflake_schema" "raw_data" {
  database = snowflake_database.demo.name
  name     = "RAW_DATA"
  comment  = "Raw ingested data from external sources"
}

resource "snowflake_schema" "ml_models" {
  database = snowflake_database.demo.name
  name     = "ML_MODELS"
  comment  = "Traditional ML models and predictions"
}

resource "snowflake_schema" "cortex_ai" {
  database = snowflake_database.demo.name
  name     = "CORTEX_AI"
  comment  = "Cortex LLM outputs and classifications"
}

resource "snowflake_schema" "dashboards" {
  database = snowflake_database.demo.name
  name     = "DASHBOARDS"
  comment  = "Views for Snowsight dashboards"
}

# Warehouses
resource "snowflake_warehouse" "ingestion_wh" {
  name           = "INGESTION_WH"
  warehouse_size = var.ingestion_warehouse_size
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Warehouse for data ingestion tasks"
}

resource "snowflake_warehouse" "ml_wh" {
  name           = "ML_WH"
  warehouse_size = var.warehouse_size
  auto_suspend   = 300
  auto_resume    = true
  comment        = "Warehouse for ML training and inference"
}

resource "snowflake_warehouse" "cortex_wh" {
  name           = "CORTEX_WH"
  warehouse_size = var.cortex_warehouse_size
  auto_suspend   = 180
  auto_resume    = true
  comment        = "Warehouse for Cortex AI functions"
}

# Tables: RAW_DATA schema

# Stock prices table
resource "snowflake_table" "stock_prices" {
  database = snowflake_database.demo.name
  schema   = snowflake_schema.raw_data.name
  name     = "STOCK_PRICES"
  
  column {
    name = "SYMBOL"
    type = "VARCHAR(10)"
  }
  
  column {
    name = "DATE"
    type = "DATE"
  }
  
  column {
    name = "OPEN"
    type = "FLOAT"
  }
  
  column {
    name = "HIGH"
    type = "FLOAT"
  }
  
  column {
    name = "LOW"
    type = "FLOAT"
  }
  
  column {
    name = "CLOSE"
    type = "FLOAT"
  }
  
  column {
    name = "VOLUME"
    type = "NUMBER(15,0)"
  }
  
  column {
    name = "INGESTED_AT"
    type = "TIMESTAMP_NTZ"
  }
  
  comment = "Stock price time series from Alpha Vantage"
}

# Real estate data table
resource "snowflake_table" "real_estate" {
  database = snowflake_database.demo.name
  schema   = snowflake_schema.raw_data.name
  name     = "REAL_ESTATE"
  
  column {
    name = "REGION_NAME"
    type = "VARCHAR(100)"
  }
  
  column {
    name = "DATE"
    type = "DATE"
  }
  
  column {
    name = "ZHVI"
    type = "FLOAT"
    comment = "Zillow Home Value Index"
  }
  
  column {
    name = "MEDIAN_SALE_PRICE"
    type = "FLOAT"
  }
  
  column {
    name = "INVENTORY_COUNT"
    type = "NUMBER"
  }
  
  column {
    name = "INGESTED_AT"
    type = "TIMESTAMP_NTZ"
  }
  
  comment = "Zillow real estate market data"
}

# SEC filings table
resource "snowflake_table" "sec_filings" {
  database = snowflake_database.demo.name
  schema   = snowflake_schema.raw_data.name
  name     = "SEC_FILINGS"
  
  column {
    name = "CIK"
    type = "VARCHAR(20)"
    comment = "Central Index Key (company identifier)"
  }
  
  column {
    name = "COMPANY_NAME"
    type = "VARCHAR(200)"
  }
  
  column {
    name = "FORM_TYPE"
    type = "VARCHAR(20)"
    comment = "8-K, S-1, etc."
  }
  
  column {
    name = "FILING_DATE"
    type = "DATE"
  }
  
  column {
    name = "FILING_TEXT"
    type = "VARCHAR"
  }
  
  column {
    name = "URL"
    type = "VARCHAR(500)"
  }
  
  column {
    name = "INGESTED_AT"
    type = "TIMESTAMP_NTZ"
  }
  
  comment = "SEC EDGAR filings for M&A and IPO events"
}

# Tables: ML_MODELS schema

# Stock price predictions table
resource "snowflake_table" "stock_predictions" {
  database = snowflake_database.demo.name
  schema   = snowflake_schema.ml_models.name
  name     = "STOCK_PREDICTIONS"
  
  column {
    name = "SYMBOL"
    type = "VARCHAR(10)"
  }
  
  column {
    name = "PREDICTION_DATE"
    type = "DATE"
  }
  
  column {
    name = "PREDICTED_CLOSE"
    type = "FLOAT"
  }
  
  column {
    name = "CONFIDENCE_INTERVAL_LOW"
    type = "FLOAT"
  }
  
  column {
    name = "CONFIDENCE_INTERVAL_HIGH"
    type = "FLOAT"
  }
  
  column {
    name = "MODEL_VERSION"
    type = "VARCHAR(50)"
  }
  
  column {
    name = "CREATED_AT"
    type = "TIMESTAMP_NTZ"
  }
  
  comment = "ML-generated stock price predictions"
}

# Tables: CORTEX_AI schema

# SEC filing summaries table
resource "snowflake_table" "filing_summaries" {
  database = snowflake_database.demo.name
  schema   = snowflake_schema.cortex_ai.name
  name     = "FILING_SUMMARIES"
  
  column {
    name     = "FILING_ID"
    type     = "NUMBER(38,0)"
    nullable = false
    identity {
      start_num = 1
      step_num  = 1
    }
  }
  
  column {
    name = "CIK"
    type = "VARCHAR(20)"
  }
  
  column {
    name = "COMPANY_NAME"
    type = "VARCHAR(200)"
  }
  
  column {
    name = "FORM_TYPE"
    type = "VARCHAR(20)"
  }
  
  column {
    name = "FILING_DATE"
    type = "DATE"
  }
  
  column {
    name = "ORIGINAL_TEXT"
    type = "VARCHAR"
  }
  
  column {
    name = "AI_SUMMARY"
    type = "VARCHAR"
  }
  
  column {
    name = "SENTIMENT"
    type = "VARCHAR(20)"
    comment = "positive, negative, neutral"
  }
  
  column {
    name = "SENTIMENT_SCORE"
    type = "FLOAT"
  }
  
  column {
    name = "CLASSIFICATION"
    type = "VARCHAR(50)"
    comment = "M&A, IPO, Restructuring, etc."
  }
  
  column {
    name = "PROCESSED_AT"
    type = "TIMESTAMP_NTZ"
  }
  
  comment = "Cortex AI-processed SEC filings"
}

# File formats
resource "snowflake_file_format" "csv_format" {
  database = snowflake_database.demo.name
  schema   = snowflake_schema.raw_data.name
  name     = "CSV_FORMAT"
  
  format_type         = "CSV"
  skip_header         = 1
  field_delimiter     = ","
  record_delimiter    = "\\n"
  field_optionally_enclosed_by = "\""
  null_if             = ["NULL", ""]
  
  comment = "CSV file format for ingestion"
}

resource "snowflake_file_format" "json_format" {
  database = snowflake_database.demo.name
  schema   = snowflake_schema.raw_data.name
  name     = "JSON_FORMAT"
  
  format_type = "JSON"
  
  comment = "JSON file format for API responses"
}

# Outputs
output "database_name" {
  value = snowflake_database.demo.name
}

output "warehouses" {
  value = {
    ingestion = snowflake_warehouse.ingestion_wh.name
    ml        = snowflake_warehouse.ml_wh.name
    cortex    = snowflake_warehouse.cortex_wh.name
  }
}

output "schemas" {
  value = {
    raw_data   = snowflake_schema.raw_data.name
    ml_models  = snowflake_schema.ml_models.name
    cortex_ai  = snowflake_schema.cortex_ai.name
    dashboards = snowflake_schema.dashboards.name
  }
}
