from fastapi import FastAPI
from pydantic import BaseModel

import uvicorn

from load_data_to_big_query import BigQuerySpark
from model import Model

app = FastAPI()

big_query = BigQuerySpark()
model = Model()

class IssueKeyword(BaseModel):
    keyword: str
    counts: int

@app.get("/most_frequent_words")
async def issues():
    issues = big_query.preprocess_issues()

    issue_keywords = model.get_issue_keyword(issues)

    return [IssueKeyword(keyword=entry[0], counts=entry[1]) for entry in issue_keywords]

@app.get("/issue_keywords")
async def issue_keywords():
    issues = big_query.preprocess_issues()

    topics = model.get_issue_keyword_LDA(issues)

    return [IssueKeyword(keyword=word, counts=int(score * 100)) for word, score in topics]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
