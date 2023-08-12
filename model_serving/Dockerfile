FROM ubuntu:latest

ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
  apt-get install -y --no-install-recommends tzdata g++ curl

# install java
RUN apt-get install -y openjdk-8-jdk
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

# install python
RUN apt-get install -y python3-pip python3-dev
RUN cd /usr/local/bin && \
  ln -s /usr/bin/python3 python && \
  ln -s /usr/bin/pip3 pip && \
  pip install --upgrade pip

# apt clean
RUN apt-get clean && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /app/

COPY ./model_serving/ /app/
COPY ./config/ /app/config/
COPY ./spark-3.3-bigquery-0.32.0.jar /app/

ENV TZ=Asia/Seoul
ENV NAVER_CLIENT_ID=YmIY_Rsg2psdCIe0N9SM
ENV NAVER_CLIENT_SECRET=1L5C03LrV4
ENV GOOGLE_CLOUD_PROJECT_ID=issue-tracker-394212
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/config/issue-tracker-394212-703a8b25244e.json

RUN pip install -r requirements.txt

CMD python app.py