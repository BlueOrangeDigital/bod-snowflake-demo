# Snowflake external stages and storage integration for S3 data ingestion
#
# Prerequisites (managed outside of OpenTofu):
#   1. An S3 bucket containing your data files
#   2. An IAM role with read access to that bucket
#      (trust policy must allow Snowflake's IAM user ARN + external ID — see outputs below)
#
# Enable with:
#   export TF_VAR_enable_s3=true
#   export TF_VAR_s3_bucket="your-bucket-name"
#   export TF_VAR_snowflake_s3_role_arn="arn:aws:iam::123456789:role/your-role"

variable "enable_s3" {
  description = "Set to true to create Snowflake storage integration and external stages"
  type        = bool
  default     = false
}

variable "s3_bucket" {
  description = "S3 bucket name for data stages"
  default     = "bod-snowflake-demo-data"
}

variable "snowflake_s3_role_arn" {
  description = "ARN of the IAM role Snowflake will assume to access S3"
  default     = ""
}

resource "snowflake_storage_integration" "s3_integration" {
  count   = var.enable_s3 ? 1 : 0
  name    = "S3_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_allowed_locations = ["s3://${var.s3_bucket}/"]
  storage_provider          = "S3"
  storage_aws_role_arn      = var.snowflake_s3_role_arn
}

resource "snowflake_stage" "stock_data_stage" {
  count               = var.enable_s3 ? 1 : 0
  database            = snowflake_database.demo.name
  schema              = snowflake_schema.raw_data.name
  name                = "STOCK_DATA_STAGE"
  url                 = "s3://${var.s3_bucket}/stock-data/"
  storage_integration = snowflake_storage_integration.s3_integration[0].name
  comment             = "Stage for stock price data from Alpha Vantage"
}

resource "snowflake_stage" "real_estate_stage" {
  count               = var.enable_s3 ? 1 : 0
  database            = snowflake_database.demo.name
  schema              = snowflake_schema.raw_data.name
  name                = "REAL_ESTATE_STAGE"
  url                 = "s3://${var.s3_bucket}/real-estate/"
  storage_integration = snowflake_storage_integration.s3_integration[0].name
  comment             = "Stage for Zillow real estate data"
}

resource "snowflake_stage" "sec_filings_stage" {
  count               = var.enable_s3 ? 1 : 0
  database            = snowflake_database.demo.name
  schema              = snowflake_schema.raw_data.name
  name                = "SEC_FILINGS_STAGE"
  url                 = "s3://${var.s3_bucket}/sec-filings/"
  storage_integration = snowflake_storage_integration.s3_integration[0].name
  comment             = "Stage for SEC EDGAR filings"
}

output "s3_bucket_name" {
  value = var.enable_s3 ? var.s3_bucket : null
}

# After enabling S3, use these values to configure the IAM role trust policy:
output "snowflake_iam_user_arn" {
  value = var.enable_s3 ? snowflake_storage_integration.s3_integration[0].storage_aws_iam_user_arn : null
}

output "snowflake_external_id" {
  value = var.enable_s3 ? snowflake_storage_integration.s3_integration[0].storage_aws_external_id : null
}
