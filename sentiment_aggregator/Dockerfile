FROM python:3.9.7-slim
RUN pip install pika
COPY /sentiment_aggregator /src
COPY /common /src/common
WORKDIR /src
ENTRYPOINT ["python3", "main.py"]