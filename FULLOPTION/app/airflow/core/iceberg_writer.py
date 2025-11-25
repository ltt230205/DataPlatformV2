from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import date, timedelta
import json


class IcebergWriter:
    # @staticmethod
    # def write_scd2_streaming(spark: SparkSession, df: DataFrame, actions: list):
    #     silver_current = df["target"].filter(F.col("valid_to") == "9999-12-31")
    #     thutucot = silver_current.columns
    #     update_df = (
    #         df["source"]
    #         .alias("src")
    #         .join(silver_current.drop("valid_to").alias("tar"), on="id")
    #         .filter(F.col("src.action").isin("update", "delete"))
    #         .select("tar.*", F.col("src.update_timestamp").alias("valid_to"))
    #     )
    #     update_df = update_df.select(thutucot)
    #     insert_df = (
    #         df["source"]
    #         .filter(F.col("action").isin("insert", "update"))
    #         .withColumn("valid_from", F.col("update_timestamp"))
    #         .withColumn("valid_to", F.lit("9999-12-31"))
    #         .withColumn("partition_date", F.current_date())
    #         .drop("update_timestamp")
    #     )
    #     insert_df = insert_df.select(thutucot)
    #     unchanged_df = silver_current.alias("tar").join(
    #         df["source"].alias("src"), on="id", how="left_anti"
    #     )
    #     final_df = unchanged_df.unionByName(update_df).unionByName(insert_df)
    #     return final_df
    
    @staticmethod
    def write_scd2(spark: SparkSession, df: DataFrame, args: dict):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
        if not spark.catalog.tableExists(args["table_name"] + "_scd2"):
            df.writeTo(args["table_name"] + "_scd2").using("iceberg").partitionedBy(
            args["partition_by"]
        ).createOrReplace()
        else:
            df.writeTo(args["table_name"] + "_scd2").using("iceberg").partitionedBy(
            args["partition_by"]
        ).append()
            
    @staticmethod #Load bảng scd4_hist
    def write_scd4_hist(spark: SparkSession, df: DataFrame, args: dict):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
        if not spark.catalog.tableExists(args["tbl_hist"]):
            df.writeTo(args["tbl_hist"]).using("iceberg").partitionedBy(
            args["partition_by"]
        ).createOrReplace()
        else:
            df.writeTo(args["table_name"] + "_scd4_hist").using("iceberg").partitionedBy(
            args["partition_by"]
        ).append()
            
        df_query=spark.sql(f"""
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_timestamp DESC) AS rn
            FROM {args["tbl_hist"]}

        ) t
        WHERE rn = 1 and (action != 'DELETE');
        """
        )
        df_query= df_query.drop("rn")
        df_query.writeTo(args["tbl_current"]).using("iceberg").partitionedBy(
            args["partition_by"]
        ).createOrReplace()
            
            
    @staticmethod #Load bảng scd4_current
    def write_scd4_current(spark: SparkSession, df: DataFrame, args: dict):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
        df.writeTo(args['table_name'] + "_scd4_current").using("iceberg").partitionedBy(
            args["partition_by"]
        ).createOrReplace()

    @staticmethod #Load bảng scd4a_hist
    def write_scd4a_hist(spark: SparkSession, df: DataFrame, args: dict):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
        if not spark.catalog.tableExists(f"{args['table_name']}_scd4a_hist"):
            df.writeTo(args["table_name"] + "_scd4a_hist").using("iceberg").partitionedBy(
            args["partition_by"]
        ).createOrReplace()
        else:
            df.writeTo(args["table_name"] + "_scd4a_hist").using("iceberg").partitionedBy(
            args["partition_by"]
        ).append()
            
    @staticmethod #Load bảng scd4a_current
    def write_scd4a_current(spark: SparkSession, df: DataFrame, args: dict):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
        df.writeTo(args['table_name'] + "_scd4a_current").using("iceberg").partitionedBy(
            args["partition_by"]
        ).createOrReplace()


