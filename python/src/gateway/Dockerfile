FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY gateway/main.py .
COPY gateway/message_handler ./message_handler

ENTRYPOINT ["python3", "main.py"]
