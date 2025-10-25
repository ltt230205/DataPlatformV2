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

yesterday = date.today() - timedelta(days=1)

df_today = spark.read.parquet(f"s3a://warehouse/bronze/customer_parquet/{yesterday}")

df_today.createOrReplaceTempView("bronze")
df_current = spark.read.format("iceberg").load("iceberg.silver.curr_cus_scd4a")
df_current.writeTo("iceberg.silver.hist1_cus_scd4a").append()
spark.sql(
    """
  MERGE INTO iceberg.silver.curr_cus_scd4a AS target
  USING (SELECT * FROM bronze) AS source
  ON target.id = source.id and target.valid_to = '9999-12-31'
  WHEN MATCHED AND source.action = 'delete' THEN
          UPDATE SET target.valid_to = source.update_timestamp,
          target.action = source.action
  WHEN MATCHED AND source.action = 'update' THEN
          UPDATE SET target.valid_to = source.update_timestamp
"""
)
spark.sql(
    f"""
  INSERT INTO iceberg.silver.curr_cus_scd4a
          select id,name,email,action,update_timestamp as valid_from,'9999-12-31' as valid_to, DATE_SUB(CURRENT_DATE(), 1) AS partition_date
          from bronze
          where action = 'insert' OR action = 'update'
"""
)
spark.sql(
    "Update iceberg.silver.curr_cus_scd4a set partition_date = DATE_SUB(CURRENT_DATE(), 1)"
)
