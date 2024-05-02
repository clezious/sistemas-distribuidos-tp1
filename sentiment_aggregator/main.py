import json
import logging
import os

from src.sentiment_aggregator import SentimentAggregator
from common.logs import initialize_log


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    input_queues = json.loads(os.getenv('INPUT_QUEUES') or '[]')
    output_queues = json.loads(os.getenv('OUTPUT_QUEUES') or '[]')

    sentiment_aggregator = SentimentAggregator(input_queues,
                                               output_queues)
    logging.info("Review mean aggregator is starting")
    sentiment_aggregator.start()


if __name__ == '__main__':
    main()
