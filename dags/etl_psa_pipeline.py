# import libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# add scrips directory to path
from scripts.extract import fetch_weather_data
from scripts.transform import process_weather_data
from scripts.load import stage_weather_data


# define dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email':['sunse523@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'timezone': 'KST',
    'retry_delay': timedelta(minutes=5)
}

with DAG('etl_psa_pipeline', default_args=default_args, start_date=datetime(2025,3,15), schedule_interval='10 * * * *', catchup=False) as dag:

    # define task
    ## fetch data from api using python operator
    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )

    # Process the data and prepare it for SQL
    process_task = PythonOperator(
        task_id='process_weather_data',
        python_callable=process_weather_data,
        provide_context=True,  # Allows access to ti (TaskInstance)
    )

    stage_task = PythonOperator(
        task_id='stage_weather_data',
        python_callable=stage_weather_data,
        provide_context=True
    )

    # dependencies
    fetch_task >> process_task >> stage_task
