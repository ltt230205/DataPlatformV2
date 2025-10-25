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
df_yesterday = spark.read.parquet(f"s3a://warehouse/bronze/customer_parquet/2025-10-05")
df_yesterday.createOrReplaceTempView("his_new")


# spark.sql("""
#   MERGE INTO iceberg.silver.hist_cus_scd4 AS target
#   USING (SELECT * FROM his_new) AS source
#   ON target.id = source.id and target.valid_to = '9999-12-31'
#   WHEN MATCHED AND source.action IN ('update', 'delete') THEN
#           UPDATE SET target.valid_to = source.update_timestamp
# """)
spark.sql(
    f"""
  INSERT INTO iceberg.silver.hist_cus_scd4
          select id,name,email,action,update_timestamp as valid_from,'9999-12-31' as valid_to,'2025-10-06' AS partition_date
          from his_new
          where action = 'delete' 
"""
)

spark.sql(
    """
    WITH hist_changes AS (
        SELECT *
        FROM iceberg.silver.hist_cus_scd4
        WHERE partition_date = '2025-10-06'
    )
    MERGE INTO iceberg.silver.curr_cus_scd4 AS target
    USING hist_changes AS source
    ON target.id = source.id and target.valid_to = '9999-12-31'
    WHEN MATCHED AND source.action = 'delete' THEN
          update SET target.valid_to = source.valid_from,
          target.action = source.action    
    WHEN MATCHED AND source.action = 'update' THEN  
        update SET target.name = source.name,
        target.email = source.email,
        target.action = source.action,
        target.valid_from = source.valid_from
    when NOT MATCHED then
          insert (id, name, email, action, valid_from, valid_to)
          values (source.id, source.name, source.email, source.action, source.valid_from, source.valid_to)
          """
)
