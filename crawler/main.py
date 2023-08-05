import datetime

from crawler import Cralwer

# 분 단위로 초를 잘라내는 함수
def truncate_seconds(dt, interval_minutes):
    truncated_minutes = int(dt.minute / interval_minutes) * interval_minutes
    return dt.replace(minute=truncated_minutes, second=0, microsecond=0)

interval_minutes = 10  # 잘라낼 분 단위

if __name__ == "__main__":
    current_time = datetime.datetime.now() -

    scrap_time = truncate_seconds(current_time, interval_minutes)
    print(f"scrap time: {scrap_time}")
    crawler = Cralwer(scrap_time=scrap_time)
    crawler.run()