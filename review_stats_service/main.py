import json
import logging
import os
import signal

from src.review_stats_service import ReviewStatsService


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    input_queues = json.loads(os.getenv('INPUT_QUEUES')) or []
    required_reviews_books_output_queue = json.loads(
        os.getenv('REQUIRED_REVIEWS_BOOKS_OUTPUT_QUEUE'))
    top_books_output_queue = json.loads(os.getenv('TOP_BOOKS_OUTPUT_QUEUE'))
    instance_id = int(os.getenv('INSTANCE_ID'))
    cluster_size = int(os.getenv('CLUSTER_SIZE'))

    stats_service = ReviewStatsService(input_queues,
                                       required_reviews_books_output_queue,
                                       top_books_output_queue,
                                       instance_id,
                                       cluster_size)
    logging.info("Review stats service %i is starting", instance_id)
    signal.signal(signal.SIGTERM, lambda signum, frame: stats_service.shutdown())
    stats_service.start()


if __name__ == '__main__':
    main()
