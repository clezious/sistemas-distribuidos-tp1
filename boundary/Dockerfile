FROM rabbitmq:3.13.1
RUN apt-get update && apt-get install -y python3
COPY boundary /
ENTRYPOINT ["/bin/sh"]