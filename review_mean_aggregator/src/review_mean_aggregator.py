import heapq
import logging
from common.book_stats import BookStats
from common.eof_packet import EOFPacket
from common.middleware import Middleware


MAX_BOOKS = 10


class ReviewMeanAggregator:
    def __init__(self,
                 input_queues: dict[str, str],
                 output_queues: list[str]):
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=output_queues,
            callback=self._save_stats,
            eof_callback=self._handle_eof,
        )
        self.books_stats: list[BookStats] = []

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()

    def _handle_eof(self, eof_packet: EOFPacket):
        result = [heapq.heappop(self.books_stats)
                  for _ in range(len(self.books_stats))]
        result.reverse()
        for book_stats in result:
            self.middleware.send(book_stats.encode())
        self.middleware.send(EOFPacket().encode())
        self.books_stats = []

    def _save_stats(self, book_stats: BookStats):
        heapq.heappush(self.books_stats, book_stats)
        if len(self.books_stats) > MAX_BOOKS:
            heapq.heappop(self.books_stats)
