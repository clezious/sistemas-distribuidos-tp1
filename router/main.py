import signal
import os
import json
from src.router import Router
from common.logs import initialize_log

initialize_log(os.getenv("LOGGING_LEVEL") or "INFO")

input_queues: dict = json.loads(os.getenv("INPUT_QUEUES") or '{}')
output_queues = json.loads(os.getenv("OUTPUT_QUEUES") or '[]')
output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES") or '[]')
hash_by_field: str = os.getenv("HASH_BY_FIELD") or None
n_instances: int = int(os.getenv("N_INSTANCES") or 1)

router = Router(input_queues, output_queues,
                output_exchanges, hash_by_field,
                n_instances)

signal.signal(signal.SIGTERM, lambda signum, frame: router.shutdown())
router.start()
