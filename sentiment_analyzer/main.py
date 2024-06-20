import json
import logging
import os
import signal
import threading

from src.sentiment_analyzer import SentimentAnalyzer
from common.logs import initialize_log
from common.health_check import HealthCheck


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    input_queues = json.loads(os.getenv('INPUT_QUEUES'))
    output_queues = json.loads(os.getenv('OUTPUT_QUEUES'))
    instance_id = int(os.getenv('INSTANCE_ID'))
    cluster_size = int(os.getenv('CLUSTER_SIZE'))

    healthcheck_port = json.loads(os.getenv("HEALTHCHECK_PORT") or '8888')

    sentiment_analyzer = SentimentAnalyzer(input_queues,
                                           output_queues,
                                           instance_id,
                                           cluster_size)
    logging.info("Review sentiment analyzer %i is starting", instance_id)
    healthcheck = HealthCheck(port=healthcheck_port)
    healthcheck_thread = threading.Thread(target=healthcheck.start)
    healthcheck_thread.start()
    signal.signal(signal.SIGTERM, lambda signum, frame: [method()
                  for method in [sentiment_analyzer.shutdown, healthcheck.shutdown, healthcheck_thread.join]])

    sentiment_analyzer.start()


if __name__ == '__main__':
    main()
