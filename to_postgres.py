import pandas as pd
from sqlalchemy import create_engine
from airflow.models import XCom  

__all__ = ['save_to_postgres']

def save_to_postgres(**kwargs):
    # CSV 파일을 Pandas DataFrame으로 로드
    # csv_file_path = '/opt/airflow/dags/crawl_data/generative_ai_data.csv'
    # df = pd.read_csv(csv_file_path)
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='get_books', key='data') 
    # PostgreSQL 연결 정보 설정
    db_username = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
    db_port = '5432'
    db_name = 'airflow'

    # SQLAlchemy 엔진 생성
    db_url = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(db_url)

    # DataFrame을 PostgreSQL 테이블로 적재
    table_name = 'yes_book'
    df.to_sql(table_name, engine, index=False, if_exists='replace')
    # 작업 완료 메시지 출력
    print(f"PostgreSQL 테이블 '{table_name}'에 성공적으로 적재되었습니다.")