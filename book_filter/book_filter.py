import logging
import os
import json

from common.book import Book
from common.middleware import Middleware

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

    def filter_book(self, book: Book):
        logging.debug(f" [x] Received {book}")
        if book.filter_by(filter_by_field, filter_by_values):
            logging.debug(" [x] Filter passed. ")
            self.middleware.send(book.encode())
        logging.debug(" [x] Done")
