FROM python:3.9.7-slim
RUN pip install pika
COPY common /common
COPY boundary /
ENTRYPOINT ["/bin/sh"]