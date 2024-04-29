import logging
from common.book import Book
from common.middleware import Middleware


class ReviewFilter:
    def __init__(self, book_input_queue: tuple[str, str], review_input_queue: tuple[str, str], output_queues: list, output_exchanges: list):
        self.middleware = Middleware(output_queues=output_queues, output_exchanges=output_exchanges)
        self.middleware.add_input_queue(book_input_queue[0], self._add_book, book_input_queue[1])
        self.middleware.add_input_queue(review_input_queue[0], self._filter_review, review_input_queue[1], auto_ack=False)
        self.books_finished = False
        self.books = {}

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()
        
    def _add_book(self, ch, method, properties, body):
        #TODO: Handle book EOF
        book = Book.decode(body)
        self.books[book.title] = book.authors
        logging.debug("Received and saved book: %s", book.title)


    def _filter_review(self, ch, method, properties, body):
        if not self.books_finished:
            logging.info("Received review but didnt get books EOF")
            self.middleware.nack(method.delivery_tag)
            return
        
        logging.info("Received review, acknowledging...")
        self.middleware.ack(method.delivery_tag)

