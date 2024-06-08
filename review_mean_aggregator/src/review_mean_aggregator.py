import heapq
import logging
from common.book_stats import BookStats
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.packet_decoder import PacketDecoder
from common.persistence_manager import PersistenceManager
import json


MAX_BOOKS = 10
BOOK_STATS_KEY = 'book_stats'


class ReviewMeanAggregator:
    def __init__(self,
                 input_queues: dict[str, str],
                 output_queues: list[str]):
        self.persistence_manager = PersistenceManager('../storage/review_mean_aggregator')
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=output_queues,
            callback=self._save_stats,
            eof_callback=self._handle_eof,
            persistence_manager=self.persistence_manager,
        )
        self.books_stats: list[BookStats] = []
        self._init_state()

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
        self.persistence_manager.put(BOOK_STATS_KEY,
                                     json.dumps([book_stats.encode() for book_stats in self.books_stats]))

    def _init_state(self):
        state = json.loads(self.persistence_manager.get(BOOK_STATS_KEY) or '[]')
        self.books_stats = [PacketDecoder.decode(book_stats) for book_stats in state]
        logging.info(f"State initialized with {[book_stats.encode() for book_stats in self.books_stats]}")
