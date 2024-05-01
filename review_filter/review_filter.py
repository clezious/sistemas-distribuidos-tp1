import logging
from common.book import Book
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.review import Review
from common.review_and_author import ReviewAndAuthor


class ReviewFilter:
    def __init__(self,
                 book_input_queue: tuple[str, str],
                 review_input_queue: tuple[str, str],
                 output_queues: list,
                 output_exchanges: list):
        self.middleware = Middleware(
            output_queues=output_queues, output_exchanges=output_exchanges)
        self.middleware.add_input_queue(
            book_input_queue[0],
            self._add_book,
            self._set_books_finished,
            book_input_queue[1],
            should_propagate_eof=False)
        self.middleware.add_input_queue(
            review_input_queue[0],
            self._filter_review,
            self._reset_filter,
            review_input_queue[1],
            auto_ack=False)
        self.books_finished = False
        self.books = {}

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()

    def _set_books_finished(self, eof_packet: EOFPacket):
        self.books_finished = True
        logging.info("Received books EOF")

    def _add_book(self, book: Book):
        self.books[book.title] = book.authors
        logging.debug("Received and saved book: %s", book.title)

    def _reset_filter(self, eof_packet: EOFPacket):
        self.books_finished = False
        self.books = {}
        logging.info("Filter reset")

    def _filter_review(self, review: Review):
        if not self.books_finished:
            logging.debug("Received review but didnt get books EOF")
            return False

        if review.book_title in self.books:
            review_and_author = ReviewAndAuthor(
                review.book_title,
                review.score,
                review.text,
                self.books[review.book_title]
            )
            self.middleware.send(review_and_author.encode())
            logging.debug("Filter passed - review for: %s", review.book_title)

        return True
