import heapq
import logging
from common.book_stats import BookStats
from common.eof_packet import EOFPacket
from common.middleware import Middleware


PERCENTILE = 90


class SentimentAggregator:
    def __init__(self,
                 input_queues: dict[str, str],
                 output_queues: list[str]):
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=output_queues,
            callback=self._save_stats,
            eof_callback=self._calculate_percentile,
        )
        self.books_stats: dict[str, dict[str, str]] = {}

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()

    def _calculate_percentile(self, eof_packet: EOFPacket):
        stats: list[BookStats] = []
        for title, book_stats in self.books_stats.items():
            average_score = book_stats["total_score"] / book_stats["total_reviews"]
            stats.append(BookStats(title, average_score))

        percentile = heapq.nlargest(
            int(len(stats) * (1 - PERCENTILE / 100)),
            stats,
            key=lambda x: x.score
        )

        for book_stats in percentile:
            self.middleware.send(book_stats.encode())
            logging.debug("Sent book stats: %s", book_stats)

        self.middleware.send(EOFPacket().encode())
        self.books_stats = {}

    def _save_stats(self, book_stats: BookStats):
        if book_stats.title not in self.books_stats:
            self.books_stats[book_stats.title] = {
                "total_score": 0,
                "total_reviews": 0,
            }
        self.books_stats[book_stats.title]["total_score"] += book_stats.score
        self.books_stats[book_stats.title]["total_reviews"] += 1
        logging.debug("Received book stats: %s", book_stats)
