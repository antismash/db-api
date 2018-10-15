# antiSMASH DB api container
# VERSION 0.1.0
FROM alpine:latest
MAINTAINER Kai Blin <kblin@biosustain.dtu.dk>

RUN apk --no-cache add python3 ca-certificates py3-psycopg2

COPY . /webapi
WORKDIR /webapi

RUN pip3 install -r /webapi/requirements.txt && pip3 install /webapi && pip3 install gunicorn

EXPOSE 8000

CMD gunicorn -b 0.0.0.0:8000 api:app
