#silver_etl.py

import sys
import logging
import boto3
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, to_timestamp


def run_silver_job():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # ---- Logging Setup ----
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f"/tmp/{args['JOB_NAME']}_{timestamp}.log"

    logger = logging.getLogger(args['JOB_NAME'])
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    s3 = boto3.client("s3")
    log_bucket = "financial-demo-sink-data"
    log_key = f"logs/silver/{args['JOB_NAME']}_{timestamp}.log"

    try:
        bronze_path = "s3://financial-demo-sink-data/bronze/facttransaction/"
        silver_path = "s3://financial-demo-sink-data/silver/facttransaction/"

        logger.info(f"Reading Bronze layer from {bronze_path}")
        df_bronze = spark.read.parquet(bronze_path)
        logger.info(f"Bronze record count: {df_bronze.count()}")

        logger.info("Transforming data for Silver layer")
        df_silver = (
            df_bronze.dropna()
            .withColumn("transaction_date", to_timestamp(col("TransactionDate"), "M/d/yyyy"))
            .select(
                col("TransactionID").alias("transaction_id"),
                col("transaction_date"),
                col("TransactionAmount").alias("transaction_amount")
            )
            .dropDuplicates(["transaction_id"])
        )
        logger.info(f"Silver record count after cleaning: {df_silver.count()}")

        logger.info(f"Writing Silver layer to {silver_path}")
        df_silver.write.mode("overwrite").parquet(silver_path)
        logger.info("Silver ETL completed successfully")

        job.commit()
        logger.info("Glue job committed successfully")

    except Exception:
        logger.error("Silver ETL failed", exc_info=True)
        raise

    finally:
        try:
            s3.upload_file(log_file, log_bucket, log_key)
            logger.info(f"Logs uploaded to s3://{log_bucket}/{log_key}")
        except Exception as e:
            print(f"Log upload failed: {str(e)}")


if __name__ == "__main__":
    run_silver_job()