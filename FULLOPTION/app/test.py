from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date, timedelta

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

# yesterday = date.today() - timedelta(days=1)
# df_check = spark.read.parquet(f"s3a://warehouse/bronze/customer_parquet_test")
# df_check.show(100)
spark.read.format("parquet").load("s3a://warehouse/bronze/customer_parquet_test").show(50)

spark.read.format("iceberg").load("iceberg.silver.customer_scd4_current").show(50)
spark.read.format("iceberg").load("iceberg.silver.customer_scd4_hist").show(50)


# his_silverdf = df_check.select("id","name","email","action",F.col("update_timestamp").alias("valid_from"),F.lit("9999-12-31").alias("valid_to")).withColumn("partition_date",F.lit("2025-10-01"))
# his_silverdf.show()
# his_silverdf.writeTo("iceberg.silver.customer2").using("iceberg").partitionedBy("partition_date").createOrReplace()
# spark.sql("DROP TABLE IF EXISTS iceberg.silver.customer_scd4_current")
# spark.sql("DROP TABLE IF EXISTS iceberg.silver.customer_scd4_hist")

# current_silverdf1=df_check.select("id","name","email","action",F.col("update_timestamp").alias("valid_from"),F.lit("9999-12-31").alias("valid_to"))
# current_silverdf1.writeTo("iceberg.silver.curr_cus_scd4").using("iceberg").createOrReplace()


# check=spark.read.format("iceberg").load("iceberg.silver.curr_cus_scd4a")
# check.show()
# his_silverdf = current_silverdf1.withColumn("partition_date",F.lit("2025-10-01"))
# his_silverdf.show()
# his_silverdf.writeTo("iceberg.silver.hist1_cus_scd4a").using("iceberg").partitionedBy("partition_date").createOrReplace()
# check = spark.read.format("iceberg").load("iceberg.silver.customer2")
# check.show()
# print("DA GHI DU LIEU XONG VAO BANG SCD4A")
