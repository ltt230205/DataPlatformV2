from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import date, timedelta
from pyspark.sql import Window
import json


class Reader:
    @staticmethod
    def read_iceberg(spark: SparkSession, args):
        latency = args.pop("latency")
        extract_date = date.today() + timedelta(days=latency)
        date_column = args.pop("date_column")
        return spark.read.format(args["file_format"]).load(args["file_path"]).filter(F.to_date(F.col(date_column)) <= F.lit(extract_date))
    
    @staticmethod
    def read_bronze(spark: SparkSession, args):
        latency = args.pop("latency")
        extract_date = date.today() + timedelta(days=latency)
        return spark.read.format(args["file_format"]).load(args["file_path"]+f"/partition_date={extract_date}")

    @staticmethod
    def read_jdbc_cdc4(spark: SparkSession, args):
        latency = args.pop("latency")
        extract_date = date.today() + timedelta(days=latency)
        date_column = args.pop("date_column")
        return spark.read.jdbc(**args).filter(F.to_date(F.col(date_column)) == F.lit(extract_date))

    @staticmethod
    def read_jdbc_cdc5(spark: SparkSession, args):
        return spark.read.jdbc(**args)