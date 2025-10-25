from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import date, timedelta
from pyspark.sql import Window
import json


class Reader:
    @staticmethod #chưa cần dùng 
    def read_file_cdc5(spark: SparkSession, args):
        extract_date = args.pop("extract_date")
        date_column = args.pop("date_column")
        return spark.read.format(args["file_format"]).load(args["file_path"]).filter(F.to_date(F.col(date_column)) == F.lit(extract_date))
    
    @staticmethod
    def read_iceberg(spark: SparkSession, args):
        extract_date = args.pop("extract_date")
        date_column = args.pop("date_column")
        return spark.read.format(args["file_format"]).load(args["file_path"]).filter(F.to_date(F.col(date_column)) <= F.lit(extract_date))
    
    @staticmethod
    def read_bronze(spark: SparkSession, args):
        return spark.read.format(args["file_format"]).load(args["file_path"]+f"/partition_date={args['extract_date']}")

    @staticmethod
    def read_jdbc(spark: SparkSession, args):
        extract_date = args.pop("extract_date")
        date_column = args.pop("date_column")
        return spark.read.jdbc(**args).filter(F.to_date(F.col(date_column)) == F.lit(extract_date))
