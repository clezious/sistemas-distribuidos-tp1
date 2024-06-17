import logging
from common.book import Book
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.review import Review
from common.review_and_author import ReviewAndAuthor
from common.persistence_manager import PersistenceManager
import json
import threading

BOOKS_KEY = 'books'
READY_TO_PROCESS_REVIEWS_KEY = 'ready_to_process_reviews'


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
        self.books_middleware = None
        self.reviews_middleware = None
        self.instance_id = instance_id
        self.cluster_size = cluster_size
        self.output_queues = output_queues
        self.output_exchanges = output_exchanges
        self.books = {}
        self.persistence_manager = PersistenceManager(f'../storage/review_filter_{review_input_queue[0]}_{book_input_queue[0]}_{instance_id}')
        self.ready_to_process_reviews = False
        self._init_state()
        self._init_books_middleware()
        self._init_reviews_middleware()
        self.books_thread = None
        self.reviews_thread = None

    def start(self):
        self.books_thread = threading.Thread(target=self.books_middleware.start)
        self.reviews_thread = threading.Thread(target=self.reviews_middleware.start)
        self.books_thread.start()
        self.reviews_thread.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.books_middleware.shutdown()
        self.reviews_middleware.shutdown()

    def _init_books_middleware(self):
        logging.info("Initializing Books Middleware")
        self.books_middleware = Middleware(
            input_queues={self.book_input_queue[0]: self.book_input_queue[1]},
            callback=self._add_book,
            eof_callback=self.handle_books_eof,
            output_queues=self.output_queues,
            output_exchanges=self.output_exchanges,
            instance_id=self.instance_id,
            persistence_manager=self.persistence_manager)

    def _init_reviews_middleware(self):
        logging.info("Initializing Reviews Middleware")
        self.reviews_middleware = Middleware(
            input_queues={
                self.review_input_queue[0]: self.review_input_queue[1]},
            callback=self._filter_review,
            eof_callback=self.handle_reviews_eof,
            output_queues=self.output_queues,
            output_exchanges=self.output_exchanges,
            instance_id=self.instance_id)

    def _start_reviews_middleware(self):
        self.persistence_manager.put(READY_TO_PROCESS_REVIEWS_KEY, 'True')
        self.ready_to_process_reviews = True

    def _add_book(self, book: Book):
        self.books[book.title] = book.authors
        self.persistence_manager.append(BOOKS_KEY, json.dumps([book.title, book.authors]))
        logging.debug("Received and saved book: %s", book.title)
        if len(self.books) % 2000 == 0:
            logging.info("Stored books count: %d", len(self.books))

    def _reset_filter(self):
        self.persistence_manager.delete_keys(prefix=BOOKS_KEY)
        self.persistence_manager.put(READY_TO_PROCESS_REVIEWS_KEY, 'False')
        self.ready_to_process_reviews = False
        self.books = {}
        logging.info("Filter reset")

    def handle_books_eof(self, eof_packet: EOFPacket):
        logging.debug(f" [x] Received Books EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)

        if len(eof_packet.ack_instances) == self.cluster_size:
            logging.debug(f" [x] Finished propagating Books EOF: {eof_packet}")
        else:
            self.books_middleware.return_eof(eof_packet)
            logging.debug(f" [x] Propagated Books EOF: {eof_packet}")

        self._start_reviews_middleware()

    def handle_reviews_eof(self, eof_packet: EOFPacket):
        if not self.ready_to_process_reviews:
            return False
        logging.debug(f" [x] Received Reviews EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)
            self._reset_filter()

        if len(eof_packet.ack_instances) == self.cluster_size:
            self.reviews_middleware.send(EOFPacket().encode())
            logging.debug(" [x] Forwarded EOF")
        else:
            self.reviews_middleware.return_eof(eof_packet)

    def _filter_review(self, review: Review):
        if not self.ready_to_process_reviews:
            return False
        if review.book_title not in self.books:
            return

        author = self.books[review.book_title]
        review_and_author = ReviewAndAuthor(
            review.book_title,
            review.score,
            review.text,
            author,
            review.trace_id)
        self.reviews_middleware.send(review_and_author.encode())
        logging.debug("Filter passed - review for: %s", review.book_title)

    def _init_state(self):
        books = self.persistence_manager.get(BOOKS_KEY).splitlines()
        for book in books:
            book = json.loads(book)
            self.books[book[0]] = book[1]
        self.ready_to_process_reviews = self.persistence_manager.get(READY_TO_PROCESS_REVIEWS_KEY) == 'True'
        logging.info(f"Initialized state with {self.books}, ready_to_process_reviews: {self.ready_to_process_reviews}")
