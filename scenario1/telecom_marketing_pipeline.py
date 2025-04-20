from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from telecom_data_generator import (
    generate_customer_demographics,
    generate_usage_patterns,
    generate_lifestyle_indicators,
    process_and_merge_data
)

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'telecom_marketing_pipeline',
    default_args=default_args,
    description='Telecom Marketing Data Pipeline',
    schedule_interval=timedelta(days=1),
)

# Define tasks
generate_demographics_task = PythonOperator(
    task_id='generate_demographics',
    python_callable=generate_customer_demographics,
    dag=dag,
)

generate_usage_task = PythonOperator(
    task_id='generate_usage_patterns',
    python_callable=generate_usage_patterns,
    dag=dag,
)

generate_lifestyle_task = PythonOperator(
    task_id='generate_lifestyle_indicators',
    python_callable=generate_lifestyle_indicators,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_and_merge_data',
    python_callable=process_and_merge_data,
    dag=dag,
)

# Set task dependencies
[generate_demographics_task, generate_usage_task, generate_lifestyle_task] >> process_data_task