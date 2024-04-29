import signal
import os
import json
from common.book import Book
from common.middleware import Middleware


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
        print(" [x] Graceful shutdown")
        self.middleware.shutdown()

    def filter_book(self, ch, method, properties, body):
        book = Book.decode(body)
        print(f" [x] Received {book}")
        if book.filter_by(filter_by_field, filter_by_values):
            print(" [x] Filter passed. ")
            self.middleware.send(book.encode())
        print(" [x] Done")


filter_by_field: str = json.loads(os.getenv("FILTER_BY_FIELD")) or ''
filter_by_values: list = json.loads(os.getenv("FILTER_BY_VALUES")) or []
input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []
output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []

book_filter = BookFilter(input_queues, output_queues, output_exchanges)

signal.signal(signal.SIGTERM, lambda signum, frame: book_filter.shutdown())
book_filter.start()
