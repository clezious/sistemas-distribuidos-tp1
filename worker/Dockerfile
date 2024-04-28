FROM python:3.9.7-slim
COPY ./requirements.txt /server/requirements.txt
RUN pip install -r /server/requirements.txt
WORKDIR /server
CMD ["python3", "main.py"]