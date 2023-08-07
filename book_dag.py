# 필요한 모듈 Import 
from datetime import datetime
import json
from airflow import DAG
import pandas as pd
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from crawl_books import get_books
from to_postgres import save_to_postgres


default_args = {
    "start_date" : datetime(2023, 1, 1)
}

def _complete():
    print("yes24_top_200_수집 완료")

# DAG 설정
with DAG(
    dag_id="yes24_book_pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    tags=['book', 'yes24', 'api'],
    catchup=False,
) as dag:

    creating_table = PostgresOperator(
        task_id="creating_table",
        postgres_conn_id="airflow-superset",
        sql='''
        CREATE TABLE IF NOT EXISTS yes_book(
        rank integer,
        title TEXT,
        author text,
        price integer,
        publishing_house text,
        publication_date text,
        review_count integer
        )
        '''
    )

    get_data_result = PythonOperator(
        task_id="get_books",
        python_callable=get_books,
        provide_context=True,  # 이 값을 True로 설정하여 컨텍스트(포함하여 XCom)를 함수로 전달합니다
        dag=dag  # DAG를 작업에 전달합니다
    )
    

    save_postgres = PythonOperator(
        task_id="save_postgres",
        python_callable=save_to_postgres,
        # provide_context=True,  # 이 값을 True로 설정하여 컨텍스트(포함하여 XCom)를 함수로 전달합니다
        dag=dag  # DAG를 작업에 전달합니다
    )


    
    # 대그 완료 출력
    print_complete = PythonOperator(
            task_id="print_complete",
            python_callable=_complete # 실행할 파이썬 함수
    )
creating_table >> get_data_result >> save_postgres >> print_complete


