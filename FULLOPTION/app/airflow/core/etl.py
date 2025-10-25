from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import date, timedelta
import json
from core import parquet_writer
from core import reader, iceberg_writer
from core import util
from pyspark.sql import Window


Reader = reader.Reader

Parquet = parquet_writer.ParquetWriter

Iceberg = iceberg_writer.IcebergWriter


class ETL:

    def create_spark():
        spark = (
            SparkSession.builder.appName("PostgresToBronze")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.27")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
            .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse")
            .getOrCreate()
        )
        return spark

    def extract_dfs(spark: SparkSession, args: dict, current_date):
        ds_df = {}
        for table_name, source in args.items():
            latency = source.pop("latency")
            extract_date = current_date + timedelta(days=latency)
            source["extract_date"] = extract_date
            function_name = source.pop("type")
            func = getattr(Reader, function_name)
            ds_df[table_name] = func(spark, source)
        return ds_df

    def extract(spark: SparkSession, args: dict, current_date):
        latency = args.pop("latency")
        extract_date = current_date + timedelta(days=latency)
        args["extract_date"] = extract_date
        function_name = args.pop("type")
        func = getattr(Reader, function_name)
        df = func(spark, args)
        return df

    def tranform_dfs(spark: SparkSession, dfs: dict, transform_conf: dict):
        for table_name, source in transform_conf.items():
            for step in source:
                func_name = step["op"]
                args = step.get("args", {})
                for k, v in args.items():
                    if isinstance(v, str) and v.startswith("F."):
                        args[k] = eval(v)
                if func_name == "sql" in args:
                    dfs[table_name].createOrReplaceTempView(args["view_name"])
                    dfs[table_name] = spark.sql(args["query"])
                if func_name == "drop" and "cols" in args:
                    dfs[table_name] = dfs[table_name].drop(*args["cols"])
                else:
                    dfs[table_name] = getattr(dfs[table_name], func_name)(**args)
        return dfs
    
    def tranform(spark: SparkSession, df: DataFrame, transform_conf: list):
        for step in transform_conf:
            func_name = step["op"]
            args = step.get("args", {})
            for k, v in args.items():
                if isinstance(v, str) and v.startswith("F."):
                    args[k] = eval(v)
            df = getattr(df, func_name)(**args)
        return df

    def load(spark: SparkSession, dfs : dict, args: dict):
        for table_name, table_config in args.items():
            print("start load table: ", table_name)
            function_name = "write_" + table_config.pop("type")
            format_type = table_config.get("format")
            executor = None
            if format_type == "parquet":
                executor = Parquet
            elif format_type == "iceberg":
                executor = Iceberg
            func = getattr(executor, function_name)
            func(spark, dfs[table_name], table_config)
            print("done load table: ", table_name)

    def run(spark: SparkSession, json_conf):
            current_date = date.today() 
            df_raw = ETL.extract_dfs(spark, json_conf["extract_dfs"], current_date)
            df_trans = ETL.tranform_dfs(spark, df_raw, json_conf["transform_dfs"])
            ETL.load(spark, df_trans, json_conf["load"])


def run(json_path: str):
    spark = ETL.create_spark()
    conf = util.read_json(json_path)
    ETL.run(spark, conf)
