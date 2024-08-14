from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
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
    'kafka_producer_dag',
    default_args=default_args,
    description='DAG to run Kafka producer every 5 minutes and trigger Spark consumer',
    schedule_interval=timedelta(minutes=5),
)

def run_kafka_producer():
    os.system('python C:\Final_Project\Scripts\kafka_producer.py')

# Define the tasks
run_kafka_producer_task = PythonOperator(
    task_id='run_kafka_producer',
    python_callable=run_kafka_producer,
    dag=dag,
)

trigger_spark_consumer_task = TriggerDagRunOperator(
    task_id='trigger_spark_consumer',
    trigger_dag_id='spark_consumer_dag',
    dag=dag,
)

# Set task dependencies
run_kafka_producer_task >> trigger_spark_consumer_task
