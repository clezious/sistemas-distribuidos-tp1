import json
import logging
import os
import signal

from book_filter import BookFilter


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


initialize_log(os.getenv("LOGGING_LEVEL") or "INFO")

input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []
output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []

book_filter = BookFilter(input_queues, output_queues, output_exchanges)

signal.signal(signal.SIGTERM, lambda signum, frame: book_filter.shutdown())
book_filter.start()
