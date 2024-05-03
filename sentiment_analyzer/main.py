import json
import logging
import os
import signal

from src.sentiment_analyzer import SentimentAnalyzer


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
    input_queues = json.loads(os.getenv('INPUT_QUEUES'))
    output_queues = json.loads(os.getenv('OUTPUT_QUEUES'))
    instance_id = int(os.getenv('INSTANCE_ID'))
    cluster_size = int(os.getenv('CLUSTER_SIZE'))

    sentiment_analyzer = SentimentAnalyzer(input_queues,
                                           output_queues,
                                           instance_id,
                                           cluster_size)
    logging.info("Review sentiment analyzer %i is starting", instance_id)
    signal.signal(signal.SIGTERM, lambda signum, frame: sentiment_analyzer.shutdown())

    sentiment_analyzer.start()


if __name__ == '__main__':
    main()
