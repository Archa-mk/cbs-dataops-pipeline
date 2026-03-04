import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)  
job.init(args['JOB_NAME'], args)

# Read CSV
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3://financial-demo-source-data/fact-transaction/")

# Minimal transformation
df_cleaned = df.dropna() \
    .withColumn("transaction_date", to_timestamp(col("TransactionDate")))
    
df_cleaned= df_cleaned.select(col('TransactionID'), col('TransactionDate'),col('TransactionAmount'))

# Write as Parquet
df_cleaned.write.mode("overwrite") \
    .parquet("s3://financial-demo-sink-data/facttransaction-clean/")

job.commit()