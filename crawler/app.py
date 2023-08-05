from fastapi import FastAPI
import uvicorn

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from crawler import Cralwer

import datetime

app = FastAPI()
scheduler = BackgroundScheduler()

def truncate_seconds(dt, interval_minutes):
    truncated_minutes = int(dt.minute / interval_minutes) * interval_minutes
    return dt.replace(minute=truncated_minutes, second=0, microsecond=0)

def crawl_job():
    interval_minutes = 10
    current_time = datetime.datetime.now()
    scrap_time = truncate_seconds(current_time, interval_minutes) - datetime.timedelta(minutes=10)

    print(f"scrap time: {scrap_time}")
    crawler = Cralwer(scrap_time=scrap_time)
    crawler.run()

@app.post("/crawl")
async def crawl():
    print("Endpoint called - ", end='')
    crawl_job()

    return {"message": "Crawling started successfully."}

if __name__ == "__main__":
    scheduler.add_job(crawl_job, CronTrigger(minute="0,10,20,30,40,50"))  # Schedule the job every 10 minutes
    scheduler.start()

    uvicorn.run(app, host="0.0.0.0", port=8000)
