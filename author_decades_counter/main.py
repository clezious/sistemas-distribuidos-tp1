import json
import logging
import os

from src.decade_counter import DecadeCounter
from common.logs import initialize_log


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
    output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []
    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
    cluster_size = json.loads(os.getenv("CLUSTER_SIZE") or '0')

    counter = DecadeCounter(input_queues=input_queues, output_queues=output_queues, instance_id=instance_id, cluster_size=cluster_size)
    logging.info("Decade counter starting")
    counter.start()


if __name__ == '__main__':
    main()
