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
        for book_stats in self.books_stats:
            self.middleware.send(book_stats.encode())
        self.middleware.send(EOFPacket().encode())
        self.persistence_manager.delete_keys(BOOK_STATS_KEY)
        self.books_stats = []

    def _save_stats(self, book_stats: BookStats):
        # Only save the new packet if it is not already in the list.
        # If it is, it means that the packet was already processed but not acknowledged.
        if book_stats.trace_id not in [stats.trace_id for stats in self.books_stats]:
            # If the list is not full, add the new packet
            if len(self.books_stats) < MAX_BOOKS:
                self.books_stats.append(book_stats)
            # If the list is full, replace the smallest packet with the new one (if the new one is bigger)
            elif self.books_stats[-1] < book_stats:
                self.books_stats[-1] = book_stats
            # If the new packet is smaller than the smallest packet, we dont have to change anything
            else:
                return

            # Sort the list after adding the new packet, then persist the state
            self.books_stats.sort(reverse=True)
            self.persistence_manager.put(BOOK_STATS_KEY,
                                         json.dumps([book_stats.encode() for book_stats in self.books_stats]))

    def _init_state(self):
        state = json.loads(self.persistence_manager.get(BOOK_STATS_KEY) or '[]')
        self.books_stats = [PacketDecoder.decode(book_stats) for book_stats in state]
        logging.info(f"State initialized with {[book_stats.encode() for book_stats in self.books_stats]}")
