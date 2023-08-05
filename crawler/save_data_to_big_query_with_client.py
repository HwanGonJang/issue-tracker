from google.cloud import bigquery
from google.oauth2 import service_account

import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Credentials 객체 생성
credentials = service_account.Credentials.from_service_account_file(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))
# 빅쿼리 클라이언트 객체 생성
client = bigquery.Client(credentials = credentials, project = credentials.project_id)

# 데이터를 삽입할 테이블 정보
project_id = os.environ.get('GOOGLE_CLOUD_PROJECT_ID')
dataset_id = "news"
table_id = "news"

# 테이블 ID
table_id = f"{project_id}.{dataset_id}.{table_id}"

def save_data(df: pd.DataFrame):
    # 테이블 객체 생성
    table = client.get_table(table_id)
    # 데이터프레임을 테이블에 삽입
    client.load_table_from_dataframe(df, table)

    print("Successfully saved data to Big Query")

