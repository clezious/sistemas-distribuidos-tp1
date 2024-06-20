import json
import logging
import os
import signal
import threading

from src.review_stats_service import ReviewStatsService
from common.logs import initialize_log
from common.health_check import HealthCheck


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    input_queues = json.loads(os.getenv('INPUT_QUEUES')) or []
    required_reviews_books_output_queue = json.loads(
        os.getenv('REQUIRED_REVIEWS_BOOKS_OUTPUT_QUEUE'))
    top_books_output_queue = json.loads(os.getenv('TOP_BOOKS_OUTPUT_QUEUE'))
    instance_id = int(os.getenv('INSTANCE_ID'))
    cluster_size = int(os.getenv('CLUSTER_SIZE'))

    healthcheck_port = json.loads(os.getenv("HEALTHCHECK_PORT") or '8888')

    stats_service = ReviewStatsService(input_queues,
                                       required_reviews_books_output_queue,
                                       top_books_output_queue,
                                       instance_id,
                                       cluster_size)
    logging.info("Review stats service %i is starting", instance_id)
    healthcheck = HealthCheck(port=healthcheck_port)
    healthcheck_thread = threading.Thread(target=healthcheck.start, daemon=True)
    healthcheck_thread.start()
    signal.signal(signal.SIGTERM, lambda signum, frame: [method()
                  for method in [stats_service.shutdown, healthcheck.shutdown, healthcheck_thread.join]])
    stats_service.start()


if __name__ == '__main__':
    main()
