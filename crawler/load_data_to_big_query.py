from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, udf

import os
from datetime import datetime, timedelta

# 구글 클라우드 프로젝트 ID 설정
os.environ["GOOGLE_CLOUD_PROJECT"] = "issue-tracker-394212"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../config/issue-tracker-394212-703a8b25244e.json"

spark = SparkSession.builder\
  .appName('issue-tracker')\
  .config('spark.jars', '../spark-3.3-bigquery-0.32.0.jar')\
  .getOrCreate()

# UDF 예시
@udf('string')
def datetime_to_string(dt):
  dt -= timedelta(hours=9)
  return dt.strftime('%Y-%m-%d %H:%M:%S')

spark.udf.register("datetime_to_string", datetime_to_string)

# 데이터 조회
df = spark.read \
  .format('bigquery') \
  .load('issue-tracker-394212.news.news')

df.createOrReplaceTempView("news")

query = """
SELECT
    id,
    title,
    content,
    to_timestamp(datetime_to_string(article_written_at)) as article_written_at,
    to_timestamp(datetime_to_string(article_written_at)) as scraped_at,
    category,
    hits,
    url
FROM
    news
WHERE
    to_timestamp(datetime_to_string(article_written_at)) >= to_timestamp('2023-07-17 14:30:00')
    AND to_timestamp(datetime_to_string(article_written_at)) <= to_timestamp('2023-07-17 14:39:59')
    AND hits >= 0
"""

# Execute the SQL query
comb_df = spark.sql(query)
comb_df.show()

print("Data loaded to BigQuery.")