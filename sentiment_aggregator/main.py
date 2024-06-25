import json
import logging
import os
import signal
import threading

from src.sentiment_aggregator import SentimentAggregator
from common.logs import initialize_log
from common.health_check import HealthCheck


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    input_queues = json.loads(os.getenv('INPUT_QUEUES') or '[]')
    output_queues = json.loads(os.getenv('OUTPUT_QUEUES') or '[]')

    healthcheck_port = json.loads(os.getenv("HEALTHCHECK_PORT") or '8888')

    sentiment_aggregator = SentimentAggregator(input_queues,
                                               output_queues)
    logging.info("Review mean aggregator is starting")
    healthcheck = HealthCheck(port=healthcheck_port)
    healthcheck_thread = threading.Thread(target=healthcheck.start, daemon=True)
    healthcheck_thread.start()
    signal.signal(signal.SIGTERM, lambda signum, frame: [method()
                  for method in [sentiment_aggregator.shutdown, healthcheck.shutdown, healthcheck_thread.join]])
    sentiment_aggregator.start()
    logging.info("Sentiment aggregator stopped")


if __name__ == '__main__':
    main()
