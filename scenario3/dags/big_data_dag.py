from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    'bigdata_dag',
    start_date=datetime(2025, 4, 18),
    schedule_interval='@daily',
    catchup=False
) as dag:
    ingest_task = SparkSubmitOperator(
        task_id='ingest_yelp_data',
        application='/spark-scripts/ingest_yelp_data.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-hive_2.12:3.2.0',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.hadoop.fs.defaultFS': 'hdfs://namenode:9000'
        },
        executor_memory='2g',
        executor_cores=2,
        num_executors=2
    )

    process_task = SparkSubmitOperator(
        task_id='process_yelp_data',
        application='/spark-scripts/spark-etl.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-hive_2.12:3.2.0',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.hadoop.fs.defaultFS': 'hdfs://namenode:9000'
        },
        executor_memory='2g',
        executor_cores=2,
        num_executors=2
    )

    ingest_task >> process_task