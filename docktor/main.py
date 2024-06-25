import json
import logging
import os
import signal
import threading

from src.docktor import Docktor
from common.logs import initialize_log
from common.health_check import HealthCheck


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    project_name: str = os.getenv("PROJECT_NAME") or ''
    excluded_containers = json.loads(os.getenv("EXCLUDED_CONTAINERS")) or []
    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
    cluster_size = json.loads(os.getenv("CLUSTER_SIZE") or '0')
    sleep_interval = json.loads(os.getenv("SLEEP_INTERVAL") or '0.07')
    healthcheck_port = json.loads(os.getenv("HEALTHCHECK_PORT") or '8888')

    healthcheck = HealthCheck(port=healthcheck_port)
    healthcheck_thread = threading.Thread(target=healthcheck.start, daemon=True)
    healthcheck_thread.start()

    docktor = Docktor(instance_id=instance_id,
                      cluster_size=cluster_size,
                      excluded_containers=excluded_containers,
                      project_name=project_name,
                      sleep_interval=sleep_interval,
                      healthcheck_port=healthcheck_port)

    signal.signal(signal.SIGTERM, lambda signum, frame: [method() for method in [docktor.shutdown, healthcheck.shutdown, healthcheck_thread.join]])
    logging.info("Docktor starting")
    docktor.start()


if __name__ == '__main__':
    main()
