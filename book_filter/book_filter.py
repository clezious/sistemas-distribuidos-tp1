import logging
import os
import json

from common.middleware import Middleware
from common.packet import Packet, PacketType

filter_by_field: str = json.loads(os.getenv("FILTER_BY_FIELD")) or ''
filter_by_values: list = json.loads(os.getenv("FILTER_BY_VALUES")) or []


class BookFilter:
    def __init__(self,
                 input_queues: dict,
                 output_queues: list,
                 output_exchanges: list):
        self.middleware: Middleware = Middleware(input_queues,
                                                 self.filter_book,
                                                 output_queues,
                                                 output_exchanges)

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info(" [x] Graceful shutdown")
        self.middleware.shutdown()

    def filter_book(self, ch, method, properties, body):
        packet = Packet.decode(body)
        if packet.packet_type == PacketType.EOF:
            logging.info("Received EOF")
            # TODO: Make sure that every node has received the EOF
            # self.middleware.send_back(body)
            self.middleware.send(body)
            return

        book = packet.payload
        logging.debug(f" [x] Received {book}")
        if book.filter_by(filter_by_field, filter_by_values):
            logging.debug(" [x] Filter passed. ")
            self.middleware.send(packet.encode())
        logging.debug(" [x] Done")
