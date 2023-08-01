import requests
import json

client_id = "YmIY_Rsg2psdCIe0N9SM"
client_secret = "1L5C03LrV4"
url = "https://openapi.naver.com/v1/datalab/search"

def get_request_body(category: str, keyword: str, date: str) -> str:
    # 딕셔너리 생성
    data = {
        "startDate": date,
        "endDate": date,
        "timeUnit": "date",
        "keywordGroups": [
            {
                "groupName": category,
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

def check_keyword_stat(category: str, keywords:list[str], date:str) -> dict:
    """
    네이버 트렌드 점수를 계산해 반환합니다.
    Args:
        category (str): 키워드의 카테고리(스포츠, 정치, 경제 등등)
        keywords (list[str]): 모델이 추출한 그날의 키워드 집합
        date (str): 트렌드를 확인할 날짜
    Returns:
        list[int]: 해당 날짜의 각 키워드의 네이버 트렌드 점수
    """
    header = {"X-Naver-Client-Id": client_id, "X-Naver-Client-Secret": client_secret, "Content-Type": "application/json"}

    trends = {}
    for keyword in keywords:
        body = get_request_body(category, keyword, date)
        response = requests.post(url=url, headers=header, data=body.encode("utf-8"))
        response_json = response.json()

        # "title"이 category인 "data"값 가져오기
        for result in response_json['results']:
            if result['title'] == category:
                data_for_keyword = result['data']
                trends[keyword] = data_for_keyword[0]['ratio']
                continue

    return trends

print(check_keyword_stat('스포츠', ['이강인', '김민재', '김하성', '올스타전'], "2023-07-15"))
