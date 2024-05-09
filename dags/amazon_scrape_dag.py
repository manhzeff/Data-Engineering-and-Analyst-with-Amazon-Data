import json
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Thêm đường dẫn đến thư mục pipelines để import các hàm
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipelines.amazon_scrape_functions import parse_listing, custom_headers, logger, transform_amazon_data, \
    write_amazon_data

# Thiết lập các tham số mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG
dag = DAG(
    'amazon_crawl_dag',
    default_args=default_args,
    description='A simple DAG to crawl Amazon data',
    schedule_interval=timedelta(days=1),
)


# Định nghĩa hàm để chạy bởi PythonOperator
def crawl_amazon(**kwargs):
    logger.info("Starting Amazon crawl task")
    search_url = "https://www.amazon.com/s?k=ps4&crid=1WLAMH40P8E1K&sprefix=ps4%2Caps%2C841&ref=nb_sb_ss_ts-doa-p_1_3"
    visited_urls = set()
    max_products = 30
    data = parse_listing(search_url, custom_headers, visited_urls, max_products)

    if data:
        # Convert data to JSON string
        data_json = json.dumps(data)

        # Push data to XCom
        ti = kwargs['ti']
        ti.xcom_push(key='product_data', value=data_json)
        logger.info("Crawl task completed and data pushed to XCom")
    else:
        logger.error("No data scraped.")


# Định nghĩa task trong DAG sử dụng PythonOperator
crawl_task = PythonOperator(
    task_id='crawl_amazon',
    python_callable=crawl_amazon,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_amazon_data',
    python_callable=transform_amazon_data,
    provide_context=True,
    dag=dag,
)

write_task = PythonOperator(
    task_id='write_amazon_data',
    python_callable=write_amazon_data,
    provide_context=True,
    dag=dag,
)

crawl_task >> transform_task >> write_task
