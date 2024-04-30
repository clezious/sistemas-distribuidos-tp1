import logging
from common.middleware import Middleware
from common.packet import Packet, PacketType

REQUIRED_DECADES = 10


class DecadeCounter:
    def __init__(self, input_queues: dict, output_queues: list):
        self.authors: dict[str, set] = {}
        self.middleware = Middleware(
            input_queues, self.add_decade, output_queues)

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info(" [x] Graceful shutdown")
        self.middleware.shutdown()

    def add_decade(self, ch, method, properties, body):
        packet = Packet.decode(body)
        if packet.packet_type == PacketType.EOF:
            logging.info("Received EOF")
            return

        book = packet.payload
        author = book.authors[0].strip() if book.authors else None
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
