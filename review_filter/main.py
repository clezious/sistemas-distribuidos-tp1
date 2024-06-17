import json
import logging
import os
import signal
import threading

from common.logs import initialize_log
from src.review_filter import ReviewFilter
from common.health_check import HealthCheck


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    book_input_queue = json.loads(os.getenv('BOOK_INPUT_QUEUE'))
    review_input_queue = json.loads(os.getenv('REVIEW_INPUT_QUEUE'))
    output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
    cluster_size = json.loads(os.getenv("CLUSTER_SIZE") or '0')

    healthcheck_port = json.loads(os.getenv("HEALTHCHECK_PORT") or '8888')

    review_filter = ReviewFilter(book_input_queue, review_input_queue,
                                 output_queues, output_exchanges,
                                 instance_id, cluster_size)
    logging.info("Review filter starting")
    healthcheck = HealthCheck(port=healthcheck_port)
    healthcheck_thread = threading.Thread(target=healthcheck.start)
    healthcheck_thread.start()
    signal.signal(signal.SIGTERM, lambda signum, frame: [method()
                  for method in [review_filter.shutdown, healthcheck.shutdown, healthcheck_thread.join]])
    review_filter.start()


if __name__ == '__main__':
    main()
