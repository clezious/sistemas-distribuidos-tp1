import logging
from common.book import Book
from common.middleware import Middleware
from common.eof_packet import EOFPacket

REQUIRED_DECADES = 10


class DecadeCounter:
    def __init__(self,
                 input_queues: dict,
                 output_queues: list,
                 instance_id: int,
                 cluster_size: int):
        self.authors: dict[str, set] = {}
        self.instance_id = instance_id
        self.cluster_size = cluster_size

        self.middleware = Middleware(
            input_queues=input_queues,
            callback=self.add_decade,
            eof_callback=self.handle_eof,
            output_queues=output_queues,
            instance_id=instance_id)

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info(" [x] Graceful shutdown")
        self.middleware.shutdown()

    def handle_eof(self, eof_packet: EOFPacket):
        logging.info(f" [x] Received EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)

        if len(eof_packet.ack_instances) == self.cluster_size:
            self.middleware.send(EOFPacket().encode())
            logging.info(f" [x] Sent EOF: {eof_packet}")
        else:
            self.middleware.return_eof(eof_packet)

    def add_decade(self, book: Book):
        author = book.authors if book.authors else None
        if not author or not book.year:
            return

        decade = (book.year // 10) * 10
        if author not in self.authors:
            self.authors[author] = set()

        if decade in self.authors[author]:
            return

        self.authors[author].add(decade)

        if len(self.authors[author]) == REQUIRED_DECADES:
            logging.info(
                "Author %s has published books in %i different decades.",
                author, REQUIRED_DECADES)
            self.middleware.send(author.encode())
