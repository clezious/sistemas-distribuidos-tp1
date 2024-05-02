import logging
import os
import json

from common.book import Book
from common.eof_packet import EOFPacket
from common.middleware import Middleware

filter_by_field: str = json.loads(os.getenv("FILTER_BY_FIELD")) or ''
filter_by_values: list = json.loads(os.getenv("FILTER_BY_VALUES")) or []


class BookFilter:
    def __init__(self,
                 input_queues: dict,
                 output_queues: list,
                 output_exchanges: list,
                 instance_id: int,
                 cluster_size: int):
        self.middleware: Middleware = Middleware(input_queues=input_queues,
                                                 callback=self.filter_book,
                                                 eof_callback=self.handle_eof,
                                                 output_queues=output_queues,
                                                 output_exchanges=output_exchanges)
        self.instance_id = instance_id
        self.cluster_size = cluster_size

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info(" [x] Graceful shutdown")
        self.middleware.shutdown()

    def handle_eof(self, eof_packet: EOFPacket):
        logging.debug(f" [x] Received EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)

        if len(eof_packet.ack_instances) == self.cluster_size:
            self.middleware.send(EOFPacket().encode())
            logging.debug(f" [x] Sent EOF: {eof_packet}")
        else:
            self.middleware.return_eof(eof_packet)

    def filter_book(self, book: Book):
        logging.debug(f" [x] Received {book}")
        if self.filter_by(filter_by_field, filter_by_values, book):
            logging.debug(" [x] Filter passed. ")
            self.middleware.send(book.encode())
        logging.debug(" [x] Done")

    def filter_by(self, field: str, compare_values: list[str], book: Book):
        field_value = book.get(field)
        if field == 'title':
            for str in compare_values:
                if str.upper() in field_value.upper():
                    return True

        elif field == 'year' and field_value is not None:
            if field_value >= compare_values[0] and field_value <= compare_values[1]:
                return True

        else:
            for value in compare_values:
                if value in field_value:
                    return True

        return False
