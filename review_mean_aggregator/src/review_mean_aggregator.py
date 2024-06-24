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
        self.persistence_manager = PersistenceManager(
            '../storage/review_mean_aggregator')
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=output_queues,
            callback=self._save_stats,
            eof_callback=self._handle_eof,
            persistence_manager=self.persistence_manager,
        )
        self.books_stats: dict[int, list[BookStats]] = {}
        self._init_state()

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()

    def _handle_eof(self, eof_packet: EOFPacket):
        client_id = eof_packet.client_id
        for book_stats in self.books_stats.get(client_id, []):
            self.middleware.send(book_stats.encode())
        self.middleware.send(EOFPacket(
            eof_packet.client_id,
            eof_packet.packet_id
        ).encode())
        if client_id in self.books_stats:
            self.persistence_manager.delete_keys(
                f"{BOOK_STATS_KEY}_{client_id}")
            self.books_stats.pop(client_id)

    def _save_stats(self, book_stats: BookStats):
        client_id = book_stats.client_id
        if client_id not in self.books_stats:
            self.books_stats[client_id] = []
        # Only save the new packet if it is not already in the list.
        # If it is, it means that the packet was already processed but not acknowledged.
        if book_stats.packet_id not in [stats.packet_id
                                        for stats in self.books_stats
                                        [client_id]]:
            # If the list is not full, add the new packet
            if len(self.books_stats[client_id]) < MAX_BOOKS:
                self.books_stats[client_id].append(book_stats)
            # If the list is full, replace the smallest packet with the new one (if the new one is bigger)
            elif self.books_stats[client_id][-1] < book_stats:
                self.books_stats[client_id][-1] = book_stats
            # If the new packet is smaller than the smallest packet, we dont have to change anything
            else:
                return

            # Sort the list after adding the new packet, then persist the state
            self.books_stats[client_id].sort(reverse=True)
            self.persistence_manager.put(
                f"{BOOK_STATS_KEY}_{client_id}", json.dumps(
                    [book_stats.encode()
                     for book_stats in self.books_stats[client_id]]))

    def _init_state(self):
        for (key, secondary_key) in self.persistence_manager.get_keys(prefix=BOOK_STATS_KEY):
            client_id = int(key.removeprefix(f"{BOOK_STATS_KEY}_"))
            state = json.loads(self.persistence_manager.get(
                key, secondary_key) or '[]')
            self.books_stats[client_id] = [
                PacketDecoder.decode(book_stats) for book_stats in state]
        logging.info("Initialized review mean aggregator with state: ")
        for client_id, client_book_stats in self.books_stats.items():
            logging.info(
                f"client_id: {client_id}, book_stats: {[book_stats.encode() for book_stats in client_book_stats]}")
