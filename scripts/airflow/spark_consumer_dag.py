from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_consumer_dag',
    default_args=default_args,
    description='DAG to run Spark consumer after Kafka producer finishes',
    schedule_interval=None,
)

def run_spark_consumer():
    os.system('spark-submit /path/to/spark_consumer.py')

# Define the task
run_spark_consumer_task = PythonOperator(
    task_id='run_spark_consumer',
    python_callable=run_spark_consumer,
    dag=dag,
)
