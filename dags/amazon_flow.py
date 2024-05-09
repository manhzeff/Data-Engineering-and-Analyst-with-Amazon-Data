from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Assuming your project's directory structure is properly set up, imports should work as intended
from pipelines import amazon_pipelines

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'amazon_product_etl',
    default_args=default_args,
    description='A simple ETL DAG for Amazon products',
    schedule_interval='@daily',  # Adjust according to your needs, or set to None for manual triggers
    catchup=False
)

with dag:
    extract_task = PythonOperator(
        task_id='extract_amazon_data',
        python_callable=amazon_pipelines.extract_amazon_data,
        op_kwargs={
            'url': 'https://www.amazon.com/s?k=ps4&crid=1WLAMH40P8E1K&sprefix=ps4%2Caps%2C841&ref=nb_sb_ss_ts-doa-p_1_3'},
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_amazon_data',
        python_callable=amazon_pipelines.transform_amazon_data,
        provide_context=True
    )

    write_task = PythonOperator(
        task_id='write_amazon_data',
        python_callable=amazon_pipelines.write_amazon_data,
        provide_context=True
    )

    extract_task >> transform_task >> write_task
