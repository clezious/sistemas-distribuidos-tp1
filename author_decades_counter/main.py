import json
import logging
import os

from decade_counter import DecadeCounter


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
    input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
    output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []

    counter = DecadeCounter(input_queues, output_queues)
    logging.info("Decade counter starting")
    counter.start()


if __name__ == '__main__':
    main()
