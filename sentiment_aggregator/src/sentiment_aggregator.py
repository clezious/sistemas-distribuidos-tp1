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
        self.persistence_manager = PersistenceManager(
            '../storage/sentiment_aggregator')
        self.books_stats: dict[int, dict[str, dict[str, str]]] = {}
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
        client_id = eof_packet.client_id
        if client_id not in self.books_stats:
            logging.warning("No data received for client %d", client_id)
            return

        stats: list[BookStats] = []
        for title, book_stats in self.books_stats[client_id].items():
            average_score = book_stats["total_score"] / book_stats["total_reviews"]
            stats.append(
                BookStats(
                    title, average_score, client_id,
                    book_stats["packet_id"]))
        stats.sort(key=lambda x: x.score)
        percentile_90_score = stats[int(len(stats) * (PERCENTILE / 100))].score
        logging.info("90th percentile score: %f", percentile_90_score)
        percentile = [book_stats for book_stats in stats
                      if book_stats.score >= percentile_90_score]

        for book_stats in percentile:
            self.middleware.send(book_stats.encode())
            logging.info("Sent book stats: %s", book_stats)

        self.middleware.send(EOFPacket(
            client_id,
            eof_packet.packet_id
        ).encode())
        self.persistence_manager.delete_keys(f"{BOOK_STATS_PREFIX}{client_id}_")
        self.books_stats.pop(client_id)

    def _save_stats(self, book_stats: BookStats):
        client_id = book_stats.client_id
        if client_id not in self.books_stats:
            self.books_stats[client_id] = {}

        if book_stats.title not in self.books_stats[client_id]:
            self.books_stats[client_id][book_stats.title] = {
                "total_score": book_stats.score,
                "total_reviews": 1,
                "packet_id": book_stats.packet_id
            }
        else:
            # Only update state if it is not a duplicate
            # (received and saved but then shutdown and restarted before acking the message)
            if self.books_stats[client_id][book_stats.title]["packet_id"] != book_stats.packet_id:
                self.books_stats[client_id][book_stats.title]["total_score"] += book_stats.score
                self.books_stats[client_id][book_stats.title]["total_reviews"] += 1
                self.books_stats[client_id][book_stats.title]["packet_id"] = book_stats.packet_id

        key = f'{BOOK_STATS_PREFIX}{client_id}_{book_stats.title}'
        self.persistence_manager.put(key, json.dumps(
            self.books_stats[book_stats.client_id][book_stats.title]))
        logging.debug("Received book stats: %s", book_stats)

    def _init_state(self):
        for (key, secondary_key) in self.persistence_manager.get_keys(BOOK_STATS_PREFIX):
            [client_id, book_title] = key.removeprefix(BOOK_STATS_PREFIX).split('_', maxsplit=1)
            client_id = int(client_id)
            book_stats = json.loads(self.persistence_manager.get(key, secondary_key))
            if client_id not in self.books_stats:
                self.books_stats[client_id] = {}
            self.books_stats[client_id][book_title] = book_stats
        logging.info(f"Initialized with state: {self.books_stats}")
