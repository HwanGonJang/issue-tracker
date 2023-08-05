from dataclasses import dataclass

from save_data_to_big_query_with_client import save_data

from requests import Request, Response, PreparedRequest, Session
from collections import namedtuple
import json
import time
import pandas as pd
import datetime

@dataclass
class Cralwer:
    scrap_time: datetime

    url = "https://news.naver.com/main/mainNews.naver"
    date = "00:00:00"
    headers = {"Host": "news.naver.com", "User-Agent": "curl/7.64.1", "Accept": "*/*"}  # 통신 헤더. 필수요소

    Category = namedtuple("Category", ["정치", "경제", "사회", "과학", "세계"])  # 뉴스 카테고리
    categories = Category(정치=100, 경제=101, 사회=102, 과학=103, 세계=104)

    def _create_request(self, category_number: int, page: int) -> PreparedRequest:
        """
        전송할 리퀘스트를 생성합니다.

        Args:
            category_number (int): 카테고리 번호
            page (int): 페이지 번호

        Returns:
            PreparedRequest: 인코딩된  HTTP 리퀘스트 객체
        """
        params = {"sid1": category_number, "page": page, "date": self.date}  # api콜을 위한 파라미터
        req = Request(method="POST", headers=self.headers, url=self.url, params=params)  # request 생성
        prepared_request = req.prepare()  # request 정보 encoding 처리된 전송 가능한 리퀘스트 생성
        return prepared_request


    def _send_request(self, req: PreparedRequest | list[PreparedRequest]) -> list[Response]:
        """
        리퀘스트 전송

        Args:
            req (PreparedRequest | list[PreparedRequest]): HTTP 리퀘스트 객체 혹은 리퀘스트 리스트

        Returns:
            list[Response]: HTTP 응답 리스트
        """
        if not isinstance(req, list):
            req = [req]  # list로 변환

        responses = []
        with Session() as session:  # 세션을 통한 커넥션 생성
            for r in req:
                try:
                    res = session.send(r, timeout=3000)  # HTTP 전송
                    responses.append(res)
                    print(f">> Send {r.url} is completed")
                    time.sleep(0.1)  # block을 위한 sleep

                except Exception as e:
                    print(">> Send Request Failed", r.url)
                    print(e)

        return responses


    def _parse_response(self, response: Response, category: str) -> pd.DataFrame:
        """
        HTTP 응답 데이터를 파싱해 필요한 데이터만 추출합니다.

        Args:
            response (Response): HTTP 응답 객체
            category (str): 카테고리 (정치, 경제, 사회, ...)

        Raises:
            ValueError: 파싱 에러

        Returns:
            list[News]: 파싱한 뉴스 템플릿 데이터
        """
        newses = []
        get_url = (
            lambda office_id, article_id: f"https://n.news.naver.com/article/{office_id}/{article_id}"
        )  # 상세 url 생성 함수

        json_res = response.json()  # api 정보 파싱
        airs_result = json.loads(json_res["airsResult"])  # TODO nested json이 희한하게 안풀려서. 번거롭게 2번 풀어줘야함.
        category_number = str(getattr(self.categories, category))

        scrap_start_time = self.scrap_time - datetime.timedelta(minutes=10)
        for news in airs_result["result"][category_number]:
            try:
                service_time = int(news["serviceTime"]) / 1000  # 기사 작성일
                service_time = datetime.datetime.fromtimestamp(service_time)

                if not scrap_start_time < service_time <= self.scrap_time:
                    continue

                print("get data and save...")
                id = news["articleId"]  # 뉴스 아이디
                title = news["title"]
                content = news["summary"]
                office_id = news["officeId"]  # 언론사 아이디
                # office_name = news["officeName"]  # 언론사 이름
                url = get_url(office_id, id)

                data = {
                    "id": id,
                    "title": title,
                    "content": content,
                    "article_written_at": service_time,
                    "scraped_at": self.scrap_time,
                    "category": category,
                    "hits": 0,
                    "comments": 0,
                    "url": url
                }

                newses.append(data)

            except Exception as e:
                raise ValueError(">> Can't Parse Data. Response Data is not correct")  # 파싱 에러

        # Create a DataFrame with explicitly defined column types
        df = pd.DataFrame(newses)

        return df

    def run(self):
        for category in self.categories._fields:
            category_number = getattr(self.categories, category)
            print(f"category_numebr: {category} is running")
            req = [self._create_request(category_number, i) for i in range(1, 2)]  # 100페이지에 대해서 요청생성
            res = self._send_request(req)
            for r in res:
                try:
                    newses = self._parse_response(r, category)

                    if len(newses):
                        print(newses.head())  # 파싱 샘플 확인
                        save_data(newses)  # big query 저장

                except Exception as e:
                    print(e)