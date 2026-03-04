import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, sum as _sum

# ------------------------------------
# Initialize Glue Context
# ------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------
# STEP 1: READ RAW DATA (Source Layer)
# ------------------------------------
df_raw = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3://financial-demo-source-data/fact-transaction/")

# ------------------------------------
# BRONZE LAYER (Raw Preserved)
# ------------------------------------
df_raw.write.mode("overwrite") \
    .parquet("s3://financial-demo-sink-data/bronze/facttransaction/")

# ------------------------------------
# SILVER LAYER (Cleaned & Standardized)
# ------------------------------------
df_silver = df_raw.dropna() \
    .withColumn("transaction_date",
                to_timestamp(col("TransactionDate"))) \
    .select(
        col("TransactionID").alias("transaction_id"),
        col("transaction_date"),
        col("TransactionAmount").alias("transaction_amount")
    ) \
    .dropDuplicates(["transaction_id"])

df_silver.write.mode("overwrite") \
    .parquet("s3://financial-demo-sink-data/silver/facttransaction/")

# ------------------------------------
# GOLD LAYER (Business Aggregation)
# ------------------------------------
df_gold = df_silver.groupBy("transaction_date") \
    .agg(
        _sum("transaction_amount")
        .alias("total_transaction_amount")
    )

df_gold.write.mode("overwrite") \
    .parquet("s3://financial-demo-sink-data/gold/facttransaction/")

# ------------------------------------
# Commit Job
# ------------------------------------
job.commit()