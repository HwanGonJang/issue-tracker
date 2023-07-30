from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

import os
from datetime import datetime

# 구글 클라우드 프로젝트 ID 설정
os.environ["GOOGLE_CLOUD_PROJECT"] = "issue-tracker-394212"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../config/issue-tracker-394212-703a8b25244e.json"

spark = SparkSession.builder\
  .appName('issue-tracker')\
  .config('spark.jars', '../spark-3.3-bigquery-0.32.0.jar')\
  .config("spark.sql.session.timeZone", "UTC")\
  .getOrCreate()

# Sample data to create the DataFrame
data = [("120275181",
         "가장 긴 이름? ‘엔카나시온-스트랜드’, 기록 경신 임박111",
         "크리스티안 엔카나시온-스트랜드. 사진=게티이미지코리아 [동아닷컴]가장 긴 이름을 가진 선수의 메이저리그 데뷔가 임박했다. 무려 27자의 이름을 가진 크리스티안 엔카나시온-스트랜드(24, 신시내티 레즈)가 데뷔를 앞두고 있다.메이저리그 공식 홈페이지 MLB.com은 17일(이하 한국시각) 가장 긴 이름의 선수가 데뷔를 앞두고 있다고 언급했다.주인공은 신시내티의 내야 유망주 엔카나시온-스트랜드. 이름의 총 글자 수는 무려 27자에 달한다. ‘Christian Encarnacion-Strand’.이는 메이저리그 역대 최고의 기록이 된다. 지난해 시미언 우즈 리차드슨(Simeon Woods Richardson)의 22자를 크게 뛰어 넘는 기록이다.엔카나시온-스트랜드는 지난 2021년 신인 드래프트에서 미네소타 트윈스의 4라운드 지명을 받은 뒤, 지난해 신시내티로 트레이드 됐다.이후 이번 시즌 신시내티의 마이너리그 트리플A 소속으로 67경기에서 타율 0.331와 20홈런 65타점, 출루율 0.405 OPS 1.042 등을 기록했다.엔카나시온-스트랜드와 우즈 리차드슨 다음으로는 총 20자의 이름을 가진 선수들이 공동 3위를 달리고 있다. 여기에는 지난 2020년 샌프란시스코 소속으로 데뷔한 루이스 알렉산더 바사베와 현 탬파베이 레이스 소속의 크리스티안 베탄코트가 있다.동아닷컴 조성운 기자 madduxly@donga.com 기자의 다른기사 더보기",
         datetime(2023, 7, 17, 14, 36, 0),
         datetime(2023, 7, 17, 14, 40, 0),
         "SPORTS",
         0,
         "https://sports.donga.com/sports/article/all/20230717/120275180/1"
         ),
        ("120275182",
         "가장 긴 이름? ‘엔카나시온-스트랜드’, 기록 경신 임박222",
         "크리스티안 엔카나시온-스트랜드. 사진=게티이미지코리아 [동아닷컴]가장 긴 이름을 가진 선수의 메이저리그 데뷔가 임박했다. 무려 27자의 이름을 가진 크리스티안 엔카나시온-스트랜드(24, 신시내티 레즈)가 데뷔를 앞두고 있다.메이저리그 공식 홈페이지 MLB.com은 17일(이하 한국시각) 가장 긴 이름의 선수가 데뷔를 앞두고 있다고 언급했다.주인공은 신시내티의 내야 유망주 엔카나시온-스트랜드. 이름의 총 글자 수는 무려 27자에 달한다. ‘Christian Encarnacion-Strand’.이는 메이저리그 역대 최고의 기록이 된다. 지난해 시미언 우즈 리차드슨(Simeon Woods Richardson)의 22자를 크게 뛰어 넘는 기록이다.엔카나시온-스트랜드는 지난 2021년 신인 드래프트에서 미네소타 트윈스의 4라운드 지명을 받은 뒤, 지난해 신시내티로 트레이드 됐다.이후 이번 시즌 신시내티의 마이너리그 트리플A 소속으로 67경기에서 타율 0.331와 20홈런 65타점, 출루율 0.405 OPS 1.042 등을 기록했다.엔카나시온-스트랜드와 우즈 리차드슨 다음으로는 총 20자의 이름을 가진 선수들이 공동 3위를 달리고 있다. 여기에는 지난 2020년 샌프란시스코 소속으로 데뷔한 루이스 알렉산더 바사베와 현 탬파베이 레이스 소속의 크리스티안 베탄코트가 있다.동아닷컴 조성운 기자 madduxly@donga.com 기자의 다른기사 더보기",
         datetime(2023, 7, 17, 14, 44, 0),
         datetime(2023, 7, 17, 14, 50, 0),
         "SPORTS",
         0,
         "https://sports.donga.com/sports/article/all/20230717/120275180/1"
         )
        ]

# # Define the schema of the DataFrame
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("content", StringType(), True),
    StructField("article_written_at", TimestampType(), True),
    StructField("scraped_at", TimestampType(), True),
    StructField("category", StringType(), True),
    StructField("hits", IntegerType(), True),
    StructField("url", StringType(), True)
])

# Create the DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show(truncate=False)

# 데이터 삽입
df.write \
    .format("bigquery") \
    .option("writeMethod", "direct") \
    .mode("append") \
    .save("news.news")

print("Data saved to BigQuery.")