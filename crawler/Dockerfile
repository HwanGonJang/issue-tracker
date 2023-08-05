FROM python:3.10.12

WORKDIR /app/

COPY ./crawler/ /app/
COPY ./config/ /app/config/

ENV TZ=Asia/Seoul
ENV NAVER_CLIENT_ID=YmIY_Rsg2psdCIe0N9SM
ENV NAVER_CLIENT_SECRET=1L5C03LrV4
ENV GOOGLE_CLOUD_PROJECT_ID=issue-tracker-394212
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/config/issue-tracker-394212-703a8b25244e.json

RUN pip install -r requirements.txt

CMD python app.py