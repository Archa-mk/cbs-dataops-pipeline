provider "aws" {
  region = "eu-north-1"
}

# Reference existing buckets (DO NOT recreate them)
data "aws_s3_bucket" "source" {
  bucket = "financial-demo-source-data"
}

data "aws_s3_bucket" "sink" {
  bucket = "financial-demo-sink-data"
}

data "aws_s3_bucket" "scripts" {
  bucket = "financial-glue-scripts"
}

# Reference existing IAM role
data "aws_iam_role" "glue_role" {
  name = "practical-demo-role"
}

# -------------------------------
# Upload local ETL script to S3
# -------------------------------
resource "aws_s3_bucket_object" "etl_script" {
  bucket = data.aws_s3_bucket.scripts.bucket  
  key    = "transaction_etl.py"
  source = "glue/transaction_etl.py"
  etag   = filemd5("glue/transaction_etl.py")
#source = "C:/Users/Archa Kishore/cbs-dataops/glue/transaction_etl.py"
#etag   = filemd5("C:/Users/Archa Kishore/cbs-dataops/glue/transaction_etl.py")
  
}

# -------------------------------
# Create Glue Job pointing to S3 script
# -------------------------------
resource "aws_glue_job" "etl_job" {
  name     = "transaction-etl-job"
  role_arn = data.aws_iam_role.glue_role.arn

  command {
    script_location = "s3://financial-glue-scripts/transaction_etl.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
}