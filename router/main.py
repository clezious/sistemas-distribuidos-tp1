import signal
import os
import json
from src.router import Router
from common.logs import initialize_log

initialize_log(os.getenv("LOGGING_LEVEL") or "INFO")


def main():
    input_queues: dict = json.loads(os.getenv("INPUT_QUEUES") or '{}')
    output_queues = json.loads(os.getenv("OUTPUT_QUEUES") or '[]')
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES") or '[]')
    hash_by_field: str = os.getenv("HASH_BY_FIELD") or None
    n_instances: int = int(os.getenv("N_INSTANCES"))
    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
    cluster_size = json.loads(os.getenv("CLUSTER_SIZE") or '0')

    router = Router(input_queues, output_queues,
                    output_exchanges, hash_by_field,
                    instance_id, cluster_size,
                    n_instances)

    signal.signal(signal.SIGTERM, lambda signum, frame: router.shutdown())
    router.start()


if __name__ == '__main__':
    main()
