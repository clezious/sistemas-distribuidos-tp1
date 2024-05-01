import logging
from common.book import Book
from common.middleware import Middleware


class Router:
    def __init__(self,
                 input_queues: dict,
                 output_queues: list,
                 output_exchanges: list,
                 hash_by_field: str,
                 n_instances: int):
        self.middleware: Middleware = Middleware(input_queues,
                                                 self.route_by_field_hash,
                                                 output_queues,
                                                 output_exchanges,
                                                 n_instances)
        self.hash_by_field = hash_by_field
        self.n_instances = n_instances

    def start(self):
        self.middleware.start()

    def shutdown(self):
        print(" [x] Graceful shutdown")
        self.middleware.shutdown()

    def hash_and_route(self, book: Book, field_value):
        instance_id = hash(field_value) % (self.n_instances)
        self.middleware.send(book.encode(), instance_id)
        logging.debug(" [x] Routed Book to instance %d" % instance_id)

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
