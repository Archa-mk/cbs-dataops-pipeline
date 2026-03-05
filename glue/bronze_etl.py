# bronze_etl.py

import sys
import logging
import boto3
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


def run_bronze_job():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # ---- Logging Setup -----
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
    log_key = f"logs/bronze/{args['JOB_NAME']}_{timestamp}.log"

    try:
        source = "s3://financial-demo-source-data/fact-transaction/"
        target = "s3://financial-demo-sink-data/bronze/facttransaction/"

        logger.info("Reading source data")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(source)

        logger.info(f"Record count: {df.count()}")

        logger.info("Writing Bronze layer")
        df.write.mode("overwrite").parquet(target)

        logger.info("Bronze ETL completed successfully")
        job.commit()

    except Exception:
        logger.error("Bronze ETL failed", exc_info=True)
        raise

    finally:
        try:
            s3.upload_file(log_file, log_bucket, log_key)
        except Exception as e:
            print(f"Log upload failed: {str(e)}")


if __name__ == "__main__":
    run_bronze_job()