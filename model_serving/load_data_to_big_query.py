from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, udf
from pyspark.sql.dataframe import DataFrame

from dataclasses import dataclass
from google.cloud import bigquery
from google.oauth2 import service_account

import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
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

@dataclass
class BigQuerySpark:
  spark: SparkSession

  # 도커파일 경로 현재 위치로 바꾸기
  def __init__(self):
    self.spark = SparkSession.builder \
      .appName('issue-tracker') \
      .config('spark.jars', '../spark-3.3-bigquery-0.32.0.jar') \
      .getOrCreate()

    # UDF
    @udf('string')
    def datetime_to_string(dt):
      dt -= timedelta(hours=9)
      return dt.strftime('%Y-%m-%d %H:%M:%S')

    @udf('string')
    def datetime_to_string_minus(min):
      dt = datetime.now()
      dt -= timedelta(minutes=min)
      return dt.strftime('%Y-%m-%d %H:%M:%S')

    self.spark.udf.register("datetime_to_string", datetime_to_string)
    self.spark.udf.register("datetime_to_string_minus", datetime_to_string_minus)

  def _get_news_data(self) -> DataFrame:
    df = self.spark.read \
      .format('bigquery') \
      .load(table_id)

    return df

  def _query_single_day_issues(self, df) -> DataFrame:
    view = 'news'
    df.createOrReplaceTempView(view)

    # 하루 동안의 뉴스 수집
    query_single_day = f"""
    SELECT
        id,
        title,
        to_timestamp(datetime_to_string(article_written_at)) as article_written_at,
        to_timestamp(datetime_to_string(scraped_at)) as scraped_at,
        category
    FROM
        {view}
    WHERE
        to_timestamp(datetime_to_string(article_written_at)) >= to_timestamp(datetime_to_string_minus(60 * 24))
        AND to_timestamp(datetime_to_string(article_written_at)) < to_timestamp(datetime_to_string_minus(0))
    """

    # 스파크 SQL query 실행
    issue_df = self.spark.sql(query_single_day)

    return issue_df

  def _query_single_hour_issues(self, df) -> DataFrame:
    view = 'issue_single_day'
    df.createOrReplaceTempView(view)

    # 1시간 동안의 뉴스 수집
    query_single_hour = f"""
    SELECT
        id,
        title,
        article_written_at,
        scraped_at,
        category
    FROM
        {view}
    WHERE
        article_written_at >= to_timestamp(datetime_to_string_minus(60 * 3))
        AND article_written_at < to_timestamp(datetime_to_string_minus(0))
    """

    issue_hour_df = self.spark.sql(query_single_hour)

    return issue_hour_df

  def preprocess_issues(self):
    df = self._get_news_data()
    df_day = self._query_single_day_issues(df)
    df_hour = self._query_single_hour_issues(df_day)

    issue_df = df_day.select('title', 'category').toPandas()
    issue_hour_df = df_hour.select('title', 'category').toPandas()

    issues = issue_df.copy()
    for _ in range(2):
      issues = pd.concat([issues, issue_hour_df], ignore_index=True)

    return issues