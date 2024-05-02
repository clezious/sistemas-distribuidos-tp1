import json
import os
import signal

from src.book_filter import BookFilter
from common.logs import initialize_log

initialize_log(os.getenv("LOGGING_LEVEL") or "INFO")

input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []
output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []

instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
cluster_size = json.loads(os.getenv("CLUSTER_SIZE") or '0')

book_filter = BookFilter(input_queues, output_queues, output_exchanges, instance_id, cluster_size)

signal.signal(signal.SIGTERM, lambda signum, frame: book_filter.shutdown())
book_filter.start()
