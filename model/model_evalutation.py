import os
import sys
import urllib.request
import json

client_id = "YmIY_Rsg2psdCIe0N9SM"
client_secret = "1L5C03LrV4"
url = "https://openapi.naver.com/v1/datalab/search"

def get_request_body(keyword: str, date: str) -> str:
    # 딕셔너리 생성
    data = {
        "startDate": date,
        "endDate": date,
        "timeUnit": "date",
        "keywordGroups": [
            {
                "groupName": keyword,
                "keywords": [keyword]
            },
            {
                "groupName": "네이버",
                "keywords": ["네이버"]
            }
        ]
    }

    # 딕셔너리를 JSON 문자열로 변환
    return json.dumps(data)

def check_keyword_stat(keywords:list[str], date:str) -> dict:
    """
    네이버 트렌드 점수를 계산해 반환합니다.
    Args:
        keywords (list[str]): 모델이 추출한 그날의 키워드 집합
        date (str): 트렌드를 확인할 날짜
    Returns:
        list[int]: 해당 날짜의 각 키워드의 네이버 트렌드 점수
    """
    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", client_id)
    request.add_header("X-Naver-Client-Secret", client_secret)
    request.add_header("Content-Type", "application/json")

    trends = dict()
    for keyword in keywords:
        body = get_request_body(keyword, date)
        response = urllib.request.urlopen(request, data=body.encode("utf-8"))

        response_body = response.read()
        response_json = response_body.decode('utf-8')

        parsed_data = json.loads(response_json)

        # "title"이 keyword인 "data" 값 가져오기
        for result in parsed_data['results']:
            if result['title'] == keyword:
                data_for_keyword = result['data']
                trends[keyword] = data_for_keyword[0]['ratio']
                continue

    return trends

print(check_keyword_stat(['이강인', '김민재', '김하성', '올스타전'], "2023-07-15"))
