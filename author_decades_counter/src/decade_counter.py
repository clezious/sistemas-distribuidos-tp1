import logging
from common.authors import Authors
from common.book import Book
from common.middleware import Middleware
from common.eof_packet import EOFPacket
from common.persistence_manager import PersistenceManager
import json

REQUIRED_DECADES = 10
AUTHOR_PREFIX = "author_"


class DecadeCounter:
    def __init__(self,
                 input_queues: dict,
                 output_queues: list,
                 instance_id: int,
                 cluster_size: int):
        self.authors: dict[int, dict[str, set]] = {}
        self.instance_id = instance_id
        self.cluster_size = cluster_size
        self.persistence_manager = PersistenceManager(
            f'../storage/decade_counter_{instance_id}')
        # self._init_state()
        self.middleware = Middleware(
            input_queues=input_queues,
            callback=self.add_decade,
            eof_callback=self.handle_eof,
            output_queues=output_queues,
            instance_id=instance_id,
            persistence_manager=self.persistence_manager)

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info(" [x] Graceful shutdown")
        self.middleware.shutdown()

    def handle_eof(self, eof_packet: EOFPacket):
        logging.debug(f" [x] Received EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)
            self.authors = {}

        if len(eof_packet.ack_instances) == self.cluster_size:
            self.middleware.send(
                EOFPacket(
                    eof_packet.client_id,
                    eof_packet.packet_id).encode())
            logging.debug(f" [x] Sent EOF: {eof_packet}")
        else:
            self.middleware.return_eof(eof_packet)

    def add_decade(self, book: Book):
        author = book.authors if book.authors else None
        if not author or not book.year:
            return

        decade = (book.year // 10) * 10
        self.authors[book.client_id] = self.authors.get(book.client_id, {})
        self.authors[book.client_id][author] = self.authors[book.client_id].get(
            author, set())

        if decade in self.authors[book.client_id][author]:
            return

        self.authors[book.client_id][author].add(decade)
        key = f'{AUTHOR_PREFIX}{book.trace_id}'
        self.persistence_manager.put(
            key, json.dumps(list(self.authors[book.client_id][author])))

        if len(self.authors[book.client_id][author]) == REQUIRED_DECADES:
            authors_packet = Authors(
                client_id=book.client_id,
                packet_id=book.packet_id,
                authors=author
            )
            logging.info(
                "Author %s has published books in %i different decades.",
                author, REQUIRED_DECADES)
            self.middleware.send(authors_packet.encode())

    def _init_state(self):
        # FIXME
        self.authors = {}
        keys = self.persistence_manager.get_keys('author_')
        logging.info(f"Initializing state with keys: {keys}")
        for key in keys:
            author = key.strip('author_')
            self.authors[author] = json.loads(self.persistence_manager.get(key))
        logging.info(f"State initialized with {self.authors}")
