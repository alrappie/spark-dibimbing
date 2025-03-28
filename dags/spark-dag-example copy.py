from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_airflow_dag_assigment_22",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Test for spark submit",
    start_date=days_ago(1),
)

ETL = SparkSubmitOperator(
    task_id="spark_submit_task_22",
    application="/spark-scripts/spark-assigment-22.py",
    conn_id="spark_main",
    name="arrow-spark",
    dag=spark_dag,
    packages="org.postgresql:postgresql:42.2.18",
)

ETL
