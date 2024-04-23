from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from pipelines.amazon_pipelines import get_amazon_page

dag=DAG(
    dag_id='amazon_flow',
    default_args={
        "owner":"manhzeff",
        "start_date": datetime(2024,4,21),
    },
    schedule_interval: None,
    catchup:False
)


extract_data_from_amazon=PythonOperator(
    task_id="extract_data_from_amazon",
    python_callable=get_amazon_page,
    provide_context=True,
    op_kwargs={"url":"https://www.amazon.com/Bose-QuietComfort-45-Bluetooth-Canceling-Headphones/dp/B098FKXT8L"},
    dag=dag
)

