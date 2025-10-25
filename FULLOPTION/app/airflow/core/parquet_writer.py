from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import date, timedelta
import json


class ParquetWriter:
    @staticmethod
    def write_file(spark: SparkSession, df: DataFrame, args: dict):
        # "s3a://warehouse/bronze/customer_parquet_test/patition_date=2025-09-08/id=1"
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df.write.format(args["format"]).mode(args["mode"]).partitionBy(
            args["partition_by"]
        ).save(args["path"])
