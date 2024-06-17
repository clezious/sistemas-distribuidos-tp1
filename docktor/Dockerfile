FROM python:3.9.7-slim
RUN pip install docker
COPY /docktor /src
COPY /common /src/common
WORKDIR /src
ENTRYPOINT ["python3", "main.py"]