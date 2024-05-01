import json
import logging
import os

from review_filter import ReviewFilter


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
    book_input_queue = json.loads(os.getenv('BOOK_INPUT_QUEUE'))
    review_input_queue = json.loads(os.getenv('REVIEW_INPUT_QUEUE'))
    output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []

    filter = ReviewFilter(book_input_queue, review_input_queue,
                          output_queues, output_exchanges)
    logging.info("Review filter starting")
    filter.start()


if __name__ == '__main__':
    main()
