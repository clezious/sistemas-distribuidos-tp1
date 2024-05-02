import json
import logging
import os

from src.review_mean_aggregator import ReviewMeanAggregator
from common.logs import initialize_log


def main():
    initialize_log(os.getenv('LOGGING_LEVEL', 'INFO'))
    input_queues = json.loads(os.getenv('INPUT_QUEUES') or '[]')
    output_queues = json.loads(os.getenv('OUTPUT_QUEUES') or '[]')

    stats_service = ReviewMeanAggregator(input_queues,
                                         output_queues,
                                         )
    logging.info("Review mean aggregator is starting")
    stats_service.start()


if __name__ == '__main__':
    main()
