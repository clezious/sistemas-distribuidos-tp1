import logging
from common.book_stats import BookStats
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.persistence_manager import PersistenceManager
import json


PERCENTILE = 90
BOOK_STATS_PREFIX = 'book_stats_'


class SentimentAggregator:
    def __init__(self,
                 input_queues: dict[str, str],
                 output_queues: list[str]):
        self.persistence_manager = PersistenceManager('../storage/sentiment_aggregator')
        self.books_stats: dict[str, dict[str, str]] = {}
        self._init_state()
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=output_queues,
            callback=self._save_stats,
            eof_callback=self._calculate_percentile,
            persistence_manager=self.persistence_manager
        )

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()

    def _calculate_percentile(self, eof_packet: EOFPacket):
        stats: list[BookStats] = []
        for title, book_stats in self.books_stats.items():
            average_score = book_stats["total_score"] / book_stats["total_reviews"]
            stats.append(BookStats(title, average_score, book_stats["trace_id"]))
        stats.sort(key=lambda x: x.score)
        percentile_90_score = stats[int(len(stats) * (PERCENTILE / 100))].score
        logging.info("90th percentile score: %f", percentile_90_score)
        percentile = [book_stats for book_stats in stats if book_stats.score >= percentile_90_score]

        for book_stats in percentile:
            self.middleware.send(book_stats.encode())
            logging.info("Sent book stats: %s", book_stats)

        self.middleware.send(EOFPacket().encode())
        self.persistence_manager.delete_keys(BOOK_STATS_PREFIX)
        self.books_stats = {}

    def _save_stats(self, book_stats: BookStats):
        if book_stats.title not in self.books_stats:
            self.books_stats[book_stats.title] = {
                "total_score": 0,
                "total_reviews": 0,
                "trace_id": book_stats.trace_id
            }
        self.books_stats[book_stats.title]["total_score"] += book_stats.score
        self.books_stats[book_stats.title]["total_reviews"] += 1

        key = f'{BOOK_STATS_PREFIX}{book_stats.title}'
        self.persistence_manager.put(key, json.dumps(self.books_stats[book_stats.title]))
        logging.debug("Received book stats: %s", book_stats)

    def _init_state(self):
        for key in self.persistence_manager.get_keys(BOOK_STATS_PREFIX):
            book_stats = json.loads(self.persistence_manager.get(key))
            title = key.strip(BOOK_STATS_PREFIX)
            self.books_stats[title] = book_stats
        logging.info(f"Initialized with state: {self.books_stats}")
