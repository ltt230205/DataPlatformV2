from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0
}

with DAG(
    dag_id="123",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="28 5 * * *",   # chạy mỗi ngày vào 11h trưa
    catchup=False,
    tags=["scd2", "bronze-silver"],
) as dag:

    # Task chạy Spark để đọc orders.csv từ MinIO
    spark_job = SparkSubmitOperator(
        task_id="read_sql_to_iceberg_task",
        application="/opt/airflow/app/scdtype4.py",  # script Spark bạn sẽ tạo
        conn_id="spark_default",
        packages="org.postgresql:postgresql:42.7.3", 
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.367.jar,"
             "/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
        verbose=True,

    )