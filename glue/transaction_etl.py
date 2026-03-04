print("CI/CD test run final1")
import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, sum as _sum
import boto3
from datetime import datetime

# ------------------------------------
# Initialize Logging
# ------------------------------------
log_file = "/tmp/etl_log.txt"

logger = logging.getLogger("GlueETLLogger")
logger.setLevel(logging.INFO)

# Stream handler to write to file
fh = logging.FileHandler(log_file)
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Also print to stdout (Glue console still shows minimal logs)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.info("ETL Job Starting...")

# ------------------------------------
# Initialize Glue Context
# ------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 client for log upload
s3 = boto3.client("s3")
log_bucket = "financial-demo-sink-data"        # your S3 bucket for logs
log_key = f"logs/etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

try:
    # ------------------------------------
    # STEP 1: READ RAW DATA (Source Layer)
    # ------------------------------------
    logger.info("Reading raw data from S3...")
    df_raw = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv("s3://financial-demo-source-data/fact-transaction/")
    logger.info(f"Raw data count: {df_raw.count()}")

    # ------------------------------------
    # BRONZE LAYER (Raw Preserved)
    # ------------------------------------
    logger.info("Writing Bronze layer to S3...")
    df_raw.write.mode("overwrite") \
        .parquet("s3://financial-demo-sink-data/bronze/facttransaction/")
    logger.info("Bronze layer write complete")

    # ------------------------------------
    # SILVER LAYER (Cleaned & Standardized)
    # ------------------------------------
    logger.info("Cleaning and transforming data for Silver layer...")
    df_silver = df_raw.dropna() \
        .withColumn("transaction_date", to_timestamp(col("TransactionDate"))) \
        .select(
            col("TransactionID").alias("transaction_id"),
            col("transaction_date"),
            col("TransactionAmount").alias("transaction_amount")
        ) \
        .dropDuplicates(["transaction_id"])
    logger.info(f"Silver data count after cleaning: {df_silver.count()}")

    logger.info("Writing Silver layer to S3...")
    df_silver.write.mode("overwrite") \
        .parquet("s3://financial-demo-sink-data/silver/facttransaction/")
    logger.info("Silver layer write complete")

    # ------------------------------------
    # GOLD LAYER (Business Aggregation)
    # ------------------------------------
    logger.info("Aggregating data for Gold layer...")
    df_gold = df_silver.groupBy("transaction_date") \
        .agg(_sum("transaction_amount").alias("total_transaction_amount"))
    logger.info(f"Gold layer record count: {df_gold.count()}")

    logger.info("Writing Gold layer to S3...")
    df_gold.write.mode("overwrite") \
        .parquet("s3://financial-demo-sink-data/gold/facttransaction/")
    logger.info("Gold layer write complete")

    logger.info("ETL Job Completed Successfully!")

except Exception as e:
    logger.error(f"ETL Job Failed: {str(e)}")
    raise

finally:
    # ------------------------------------
    # Upload log file to S3
    # ------------------------------------
    try:
        s3.upload_file(log_file, log_bucket, log_key)
        print(f"ETL logs uploaded to s3://{log_bucket}/{log_key}")
    except Exception as e:
        print(f"Failed to upload logs to S3: {str(e)}")

# ------------------------------------
# Commit Glue Job
# ------------------------------------
job.commit()