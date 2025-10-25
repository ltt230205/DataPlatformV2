from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("PostgresToBronze")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.27")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hive")
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse")
    .getOrCreate()
)
# JDBC connection
jdbc_url = "jdbc:postgresql://postgres-source:5432/sourcedb"
connection_props = {
    "user": "source_user",
    "password": "source_pass",
    "driver": "org.postgresql.Driver",
}

# đọc bảng từ postgres
df = spark.read.jdbc(url=jdbc_url, table="public.customer", properties=connection_props)
df.show()
bronze_df = df.withColumn("action", F.lit("insert")).withColumn(
    "partition_date", F.current_date()
)
# Tạo namespace  trong Iceberg

spark.sql(
    """
   CREATE NAMESPACE IF NOT EXISTS iceberg.bronze
   LOCATION 's3a://warehouse/bronze'
"""
)

print("đã tạo namespace bronze")
# Ghi vào bảng Iceberg
(
    bronze_df.writeTo("iceberg.bronze.customer_br2")
    .using("iceberg")
    .partitionedBy("partition_date")
    .createOrReplace()
)
print("đã ghi VÀO BRONZE")

silver_df = bronze_df.select(
    "id",
    "name",
    "email",
    "action",
    F.col("update_timestamp").alias("valid_from"),
    F.lit("9999-12-31").alias("valid_to"),
).withColumn("partition_date", F.current_date())
silver_df.show()
print("đã tạo silver_df")

spark.sql(
    """
   CREATE NAMESPACE IF NOT EXISTS iceberg.silver
   LOCATION 's3a://warehouse/silver'
"""
)
print("đã tạo namespace silver")
silver_df.writeTo("iceberg.silver.customer_si").using("iceberg").partitionedBy(
    "partition_date"
).createOrReplace()
print("đã ghi VÀO SILVER")
