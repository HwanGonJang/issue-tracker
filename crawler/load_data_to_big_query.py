from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, udf

import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
project_id = os.environ.get('GOOGLE_CLOUD_PROJECT_ID')

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
  .load(f'{project_id}.news.news')

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