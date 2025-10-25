from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date, timedelta
from core.etl import ETL
import util

config = util.read_json("./config/raw2bronze_customer.json")

spark = ETL.create_spark()


def create_customer_df():
    df = spark.createDataframe()
    return df


def create_df():
    dataframes = {"public.customer": create_customer_df()}
    return dataframes


def testETL():
    dfs = create_customer_df()
    df = ETL.tranform(spark, dfs["public.customer"], config["transform"])
    df.show()


# bronze_df = df.select("id", "name", "email", "update_timestamp", "action")
# bronze_df.show()
# yesterday = date.today() - timedelta(days=1)
# (
#     bronze_df.write.mode("append").parquet(  # hoặc "overwrite"
#         f"s3a://warehouse/bronze/customer_parquet/{yesterday}"
#     )
# )


# df_check = spark.read.parquet(f"s3a://warehouse/bronze/customer_parquet/{yesterday}")
# df_check.show()
# df_check.printSchema()
