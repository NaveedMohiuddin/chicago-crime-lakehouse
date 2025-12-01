import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue context (ONLY ONCE!)
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("Starting crime data curation job...")
print(f"Spark version: {spark.version}")

# Configure Iceberg catalog
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://chi311-curated-us-east-1/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Read raw crime data from S3
print("Reading raw crime data from S3...")
raw_path = "s3://chi311-raw-us-east-1/raw/requests/*/*/*.gz"

try:
    raw_df = spark.read.json(raw_path)
    record_count = raw_df.count()
    print(f"Read {record_count} raw records")
    raw_df.printSchema()
except Exception as e:
    print(f"Error reading raw data: {e}")
    job.commit()
    sys.exit(1)

# Transform and clean data
print("Transforming data...")

crimes_df = (raw_df
    # Parse and validate dates
    .withColumn("date_ts", F.to_timestamp("date", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    .withColumn("updated_ts", F.to_timestamp("updated_on", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    
    # Extract date/time components for partitioning and analysis
    .withColumn("dt", F.date_format("date_ts", "yyyy-MM-dd"))
    .withColumn("year", F.year("date_ts"))
    .withColumn("month", F.month("date_ts"))
    .withColumn("day_of_week", F.dayofweek("date_ts"))  # 1=Sunday, 7=Saturday
    .withColumn("hour", F.hour("date_ts"))
    
    # Clean and standardize fields
    .withColumn("crime_id", F.col("id"))
    .withColumn("case_number", F.col("case_number"))
    .withColumn("block", F.col("block"))
    .withColumn("iucr", F.col("iucr"))
    .withColumn("primary_type", F.upper(F.trim(F.col("primary_type"))))
    .withColumn("description", F.col("description"))
    .withColumn("location_description", F.col("location_description"))
    .withColumn("arrest", F.col("arrest").cast("boolean"))
    .withColumn("domestic", F.col("domestic").cast("boolean"))
    .withColumn("beat", F.col("beat"))
    .withColumn("district", F.col("district"))
    .withColumn("ward", F.col("ward").cast("int"))
    .withColumn("community_area", F.col("community_area"))
    .withColumn("fbi_code", F.col("fbi_code"))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    
    # Add derived analytics columns
    .withColumn("is_violent", 
        F.when(F.col("primary_type").isin(
            "HOMICIDE", "ASSAULT", "BATTERY", "ROBBERY", 
            "CRIMINAL SEXUAL ASSAULT", "CRIM SEXUAL ASSAULT"
        ), True).otherwise(False))
    
    .withColumn("is_property", 
        F.when(F.col("primary_type").isin(
            "THEFT", "BURGLARY", "MOTOR VEHICLE THEFT", 
            "ROBBERY", "ARSON"
        ), True).otherwise(False))
    
    .withColumn("time_of_day",
        F.when(F.col("hour").between(6, 11), "Morning")
         .when(F.col("hour").between(12, 17), "Afternoon")
         .when(F.col("hour").between(18, 21), "Evening")
         .otherwise("Night"))
    
    # Keep ingestion metadata
    .withColumn("ingested_at", F.col("_ingested_at"))
    .withColumn("dedupe_id", F.col("_dedupe_id"))
)

# Select final columns
crimes_clean = crimes_df.select(
    "crime_id", "case_number", "date_ts", "updated_ts",
    "dt", "year", "month", "day_of_week", "hour", "time_of_day",
    "block", "iucr", "primary_type", "description", "location_description",
    "arrest", "domestic", "beat", "district", "ward", "community_area", "fbi_code",
    "latitude", "longitude",
    "is_violent", "is_property",
    "ingested_at", "dedupe_id"
).filter(F.col("date_ts").isNotNull())  # Filter out invalid dates

clean_count = crimes_clean.count()
print(f"Cleaned data: {clean_count} records")

# Create database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.chicrime")

# Write to Iceberg table
print("Writing to Iceberg table...")
table_name = "glue_catalog.chicrime.crimes_iceberg"

try:
    (crimes_clean
        .writeTo(table_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .partitionedBy("dt")
        .createOrReplace())
    
    print(f"Successfully wrote data to {table_name}")
    
    # Show sample data
    print("Sample records:")
    spark.table(table_name).show(5, truncate=False)
    
except Exception as e:
    print(f"Error writing to Iceberg: {e}")
    import traceback
    traceback.print_exc()
    job.commit()
    sys.exit(1)

print("Job completed successfully!")
job.commit()
