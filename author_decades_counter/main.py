import json
import logging
import os
import signal
import threading

from src.decade_counter import DecadeCounter
from common.logs import initialize_log
from common.health_check import HealthCheck


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
    output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []
    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
    cluster_size = json.loads(os.getenv("CLUSTER_SIZE") or '0')

    healthcheck_port = json.loads(os.getenv("HEALTHCHECK_PORT") or '8888')

    counter = DecadeCounter(input_queues=input_queues, output_queues=output_queues,
                            instance_id=instance_id, cluster_size=cluster_size)
    healthcheck = HealthCheck(port=healthcheck_port)
    healthcheck_thread = threading.Thread(target=healthcheck.start, daemon=True)
    healthcheck_thread.start()
    signal.signal(signal.SIGTERM, lambda signum, frame: [method() for method in [
                  counter.shutdown, healthcheck.shutdown, healthcheck_thread.join]])
    logging.info("Decade counter starting")
    counter.start()
    logging.info("Decade counter stopped")


if __name__ == '__main__':
    main()
