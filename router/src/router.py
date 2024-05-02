from common.book import Book
from common.middleware import Middleware
from common.eof_packet import EOFPacket
import logging


class Router:
    def __init__(self,
                 input_queues: dict,
                 output_queues: list,
                 output_exchanges: list,
                 hash_by_field: str,
                 instance_id: int,
                 cluster_size: int,
                 n_instances: int):
        self.middleware: Middleware = Middleware(
            input_queues=input_queues,
            callback=self.route_by_field_hash,
            eof_callback=self.handle_eof,
            output_queues=output_queues,
            output_exchanges=output_exchanges,
            n_output_instances=n_instances)
        self.instance_id = instance_id
        self.cluster_size = cluster_size
        self.hash_by_field = hash_by_field
        self.n_instances = n_instances

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
            self.middleware.send(EOFPacket().encode(), instance_id=0)
            logging.debug(f" [x] Sent EOF: {eof_packet}")
        else:
            self.middleware.return_eof(eof_packet)

    def hash_and_route(self, book: Book, field_value):
        instance_id = hash(field_value) % (self.n_instances)
        self.middleware.send(book.encode(), instance_id)
        logging.debug(f" [x] Routed Book to instance {instance_id}")

    def route_by_field_hash(self, book: Book):
        logging.debug(f" [x] Received {book}")
        field_value = book.get(self.hash_by_field)
        # if isinstance(field_value, list):
        #     for value in field_value:
        #         book.set(self.hash_by_field, [value])
        #         self.hash_and_route(book, value)
        # else:
        #     self.hash_and_route(book, field_value)
        self.hash_and_route(book, field_value)
