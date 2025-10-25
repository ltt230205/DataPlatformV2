from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date, when
from pyspark.sql.types import StructType, StructField, StringType, LongType
from core import  util
from pyspark.sql import DataFrame

class CDC:
    schema = StructType([
        StructField("payload", StructType([
            StructField("before", StructType([
                StructField("id", StringType()),
                StructField("name", StringType()),
                StructField("email", StringType())
            ])),
            StructField("after", StructType([
                StructField("id", StringType()),
                StructField("name", StringType()),
                StructField("email", StringType())
            ])),
            StructField("op", StringType()),
            StructField("source", StructType([
                StructField("ts_ms", LongType())
            ]))
        ]))
    ])
    def create_spark():
        spark = (
            SparkSession.builder
            .appName("CDC Debezium Stream Reader")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
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
    
    def read_cdc_stream(spark: SparkSession, args: dict):
        df_raw = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args["kafka_bootstrap"])
            .option("subscribe", args["topic"])
            .option("startingOffsets", args["mode"])  # dùng "earliest" nếu muốn đọc cả snapshot
            .load()
        )
        df_value = df_raw.selectExpr("CAST(value AS STRING) AS json_str")
        return df_value
    def transform_cdc_stream( df: DataFrame):
        dfjson2data = df.select(from_json(col("json_str"), CDC.schema).alias("data"))
        df_parsed = (
        dfjson2data
        .select(
            col("data.payload.op").alias("operation"),
            # Nếu là delete -> lấy from before, ngược lại -> lấy from after
            when(col("data.payload.op") == "d", col("data.payload.before.id"))
             .otherwise(col("data.payload.after.id")).alias("id"),
            when(col("data.payload.op") == "d", col("data.payload.before.name"))
             .otherwise(col("data.payload.after.name")).alias("name"),
            when(col("data.payload.op") == "d", col("data.payload.before.email"))
             .otherwise(col("data.payload.after.email")).alias("email"),
            to_timestamp(col("data.payload.source.ts_ms") / 1000).alias("updated_timestamp"),
        )
        .withColumn("partition_date", to_date(col("updated_timestamp")))
    )
        return df_parsed
    
    def write_cdc_stream(spark: SparkSession, df: DataFrame, args: dict):
        query = (
            df.writeStream
            .format(args["format"])
            .option("path", args["path"])
            .option("checkpointLocation", f"s3a://warehouse/checkpoints/customer_cdc_{args['topic']}")
            .partitionBy(args["partition_by"])
            .outputMode("append")
            .start()
        )
        return query
    
    def run_cdc_stream(spark: SparkSession, json_conf):
        df_raw = CDC.read_cdc_stream(spark, json_conf["extract"])
        df_transformed = CDC.transform_cdc_stream( df_raw)
        query = CDC.write_cdc_stream(spark, df_transformed, json_conf["load"])
        query.awaitTermination()

def run(json_path: str):
    spark = CDC.create_spark()
    conf = util.read_json(json_path)
    CDC.run_cdc_stream(spark, conf)

