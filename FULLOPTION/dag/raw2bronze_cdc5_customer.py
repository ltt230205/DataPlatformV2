from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="raw2bronze_cdc5_customer",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # chỉ chạy khi trigger
    catchup=False,
    default_args=default_args,
) as dag:

    # Task chạy Spark để đọc orders.csv từ MinIO
    spark_job = SparkSubmitOperator(
        task_id="raw2bronze_cdc5_customer",
        application="/opt/bitnami/spark/app/airflow/job/raw2bronze_customer.py",
        py_files="/opt/bitnami/spark/app/airflow/core.zip", # script Spark bạn sẽ tạo
        conn_id="spark_default", # hoặc dùng spark-thrift-server nếu bạn cấu hình connection
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