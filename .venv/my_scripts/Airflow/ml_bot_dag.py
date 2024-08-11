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
    'ml_bot_dag',
    default_args=default_args,
    description='DAG to run ML bot every 30 minutes',
    schedule_interval=timedelta(minutes=30),
)

def run_ml_bot():
    os.system('python /path/to/ml_bot.py')

# Define the task
run_ml_bot_task = PythonOperator(
    task_id='run_ml_bot',
    python_callable=run_ml_bot,
    dag=dag,
)
