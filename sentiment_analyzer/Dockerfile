FROM python:3.9.7-slim
RUN pip install pika textblob
COPY /sentiment_analyzer /src
COPY /common /src/common
WORKDIR /src
ENTRYPOINT ["python3", "main.py"]