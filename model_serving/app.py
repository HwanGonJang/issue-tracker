import json

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

@app.get("/issues")
async def issues():
    issues = big_query.preprocess_issues()

    issue_keywords = model.get_issue_keyword(issues)

    return [IssueKeyword(keyword=entry[0], counts=entry[1]) for entry in issue_keywords]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
