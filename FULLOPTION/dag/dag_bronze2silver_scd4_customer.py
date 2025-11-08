from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="bronze_to_silver_customer_scd4",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # chỉ chạy khi trigger
    catchup=False,
    default_args=default_args,
    description="Chạy 2 job Spark: customer_scd4_current và customer_scd4_hist",
) as dag:

# 1 Job tạo bảng hist
    job_hist = SparkSubmitOperator(
        task_id="bronze_to_silver_customer_scd4_hist",
        application="/opt/bitnami/spark/app/airflow/job/bronze2sil_scd4_hist_customer.py",
        py_files="/opt/bitnami/spark/app/airflow/core.zip",
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

    # 2 Job tạo bảng current
    job_current = SparkSubmitOperator(
        task_id="bronze_to_silver_customer_scd4_current",
        application="/opt/bitnami/spark/app/airflow/job/bronze2sil_scd4_current_customer.py",
        py_files="/opt/bitnami/spark/app/airflow/core.zip",
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


    # Thiết lập thứ tự chạy: current → hist
    job_hist >> job_current
