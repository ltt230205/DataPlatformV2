from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Khởi tạo SparkSession
spark = (
    SparkSession.builder.appName("IcebergTest")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hive")
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
    .config(
        "spark.sql.catalog.iceberg.warehouse", "s3a://warehouse"
    )  # optional nhưng nên có
    .getOrCreate()
)


df = spark.read.option("header", True).csv("s3a://raw/startup_data.csv")
df.show(5, truncate=False)
spark.sql(
    """
  CREATE SCHEMA IF NOT EXISTS iceberg.silver2_ICE
  LOCATION 's3a://warehouse/silver2_ICE'
"""
)

(
    df.writeTo("iceberg.silver2_ICE.startup_data")  # catalog.schema.table
    .using("iceberg")
    .createOrReplace()
)
print("đã ghi iceberg")
spark.stop()
