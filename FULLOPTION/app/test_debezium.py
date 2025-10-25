from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# 🔹 1. Tạo Spark session với Kafka support
spark = (
    SparkSession.builder.appName("CDC Debezium Stream Reader")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 🔹 2. Kafka config
kafka_bootstrap = "kafka:9092"
topic = "test.public.customer"

# 🔹 3. Đọc stream từ Kafka
df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

# 🔹 4. Giải mã JSON từ Kafka (Debezium gửi value dạng JSON string)
df_value = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# 🔹 5. Định nghĩa schema cho phần "after" của Debezium
schema = StructType(
    [
        StructField(
            "payload",
            StructType(
                [
                    StructField(
                        "after",
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("name", StringType()),
                                StructField("email", StringType()),
                                StructField("update_timestamp", StringType()),
                            ]
                        ),
                    ),
                    StructField("op", StringType()),
                ]
            ),
        )
    ]
)

# 🔹 6. Parse JSON (Debezium có cấu trúc lồng "payload" hoặc "after")
df_parsed = df_value.select(
    from_json(col("json_str"), schema=schema).alias("data")
).select("data.payload.after.*", "data.payload.op")

# 🔹 7. Hiển thị realtime trên console
query = (
    df_parsed.writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()


print("tung")