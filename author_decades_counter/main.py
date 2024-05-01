import json
import logging
import os

from src.decade_counter import DecadeCounter
from common.logs import initialize_log


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
    output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []

    counter = DecadeCounter(input_queues, output_queues)
    logging.info("Decade counter starting")
    counter.start()


if __name__ == '__main__':
    main()
