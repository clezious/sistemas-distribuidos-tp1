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
                 output_exchanges: list,
                 instance_id: int,
                 cluster_size: int):
        self.book_input_queue = book_input_queue
        self.review_input_queue = review_input_queue
        self.middleware = None
        self.instance_id = instance_id
        self.cluster_size = cluster_size
        self.output_queues = output_queues
        self.output_exchanges = output_exchanges
        self.books = {}
        self._init_middleware()

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()

    def _init_middleware(self):
        logging.info("Initializing Books Middleware")
        self.middleware = Middleware(
            input_queues={self.book_input_queue[0]: self.book_input_queue[1]},
            callback=self._add_book,
            eof_callback=self.handle_books_eof,
            output_queues=self.output_queues,
            output_exchanges=self.output_exchanges,
            instance_id=self.instance_id)
        self.middleware.add_input_queue(
            self.book_input_queue[0],
            self._add_book,
            self.handle_books_eof,
            exchange=self.book_input_queue[1])

    def _add_book(self, book: Book):
        self.books[book.title] = book.authors
        logging.debug("Received and saved book: %s", book.title)
        if len(self.books) % 2000 == 0:
            logging.info("Stored books count: %d", len(self.books))

    def _reset_filter(self):
        self.books = {}
        logging.info("Filter reset")

    def handle_books_eof(self, eof_packet: EOFPacket):
        logging.info(f" [x] Received Books EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)

        if len(eof_packet.ack_instances) == self.cluster_size:
            logging.debug(f" [x] Finished propagating Books EOF: {eof_packet}")
        else:
            self.middleware.return_eof(eof_packet)
            logging.debug(f" [x] Propagated Books EOF: {eof_packet}")

        self._switch_to_reviews()

    def handle_reviews_eof(self, eof_packet: EOFPacket):
        logging.info(f" [x] Received Reviews EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)
            self._reset_filter()

        if len(eof_packet.ack_instances) == self.cluster_size:
            self.middleware.send(EOFPacket().encode())
            logging.debug(" [x] Forwarded EOF")
        else:
            self.middleware.return_eof(eof_packet)

        self._switch_to_books()

    def _filter_review(self, review: Review):
        if review.book_title not in self.books:
            return

        author = self.books[review.book_title]
        review_and_author = ReviewAndAuthor(
            review.book_title,
            review.score,
            review.text,
            author)
        self.middleware.send(review_and_author.encode())
        logging.debug("Filter passed - review for: %s", review.book_title)

    def _switch_to_reviews(self):
        self.middleware.cancel(self.book_input_queue[0])
        self.middleware.add_input_queue(
            self.review_input_queue[0],
            self._filter_review,
            self.handle_reviews_eof,
            exchange=self.review_input_queue[1])
        logging.info("Switched to Reviews Middleware")

    def _switch_to_books(self):
        self.middleware.cancel(self.review_input_queue[0])
        self.middleware.add_input_queue(
            self.book_input_queue[0],
            self._add_book,
            self.handle_books_eof,
            exchange=self.book_input_queue[1])
        logging.info("Switched to Books Middleware")
