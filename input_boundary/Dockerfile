FROM python:3.9.7-slim
RUN pip install pika
COPY /input_boundary /src
COPY /common /src/common
WORKDIR /src
ENTRYPOINT ["python3", "main.py"]