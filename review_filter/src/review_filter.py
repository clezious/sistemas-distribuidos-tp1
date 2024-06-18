import logging
import threading
from common.book import Book
from common.eof_packet import EOFPacket
from common.middleware import CallbackAction, Middleware
from common.review import Review
from common.review_and_author import ReviewAndAuthor
from common.persistence_manager import PersistenceManager
import json

BOOKS_KEY = 'books'
EOFS_KEY = 'eofs'


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
        self.instance_id = instance_id
        self.cluster_size = cluster_size
        self.output_queues = output_queues
        self.output_exchanges = output_exchanges
        self.books: dict[int, dict[str, str]] = {}
        self.eofs: set[int] = set()
        self.persistence_manager = PersistenceManager(
            f'../storage/review_filter_{review_input_queue[0]}_{book_input_queue[0]}_{instance_id}')
        self._init_state()

        self.reviews_middleware = None
        self.books_middleware = None
        self.books_receiver = threading.Thread(target=self._books_receiver)
        self.reviews_receiver = threading.Thread(target=self._reviews_receiver)

    def start(self):
        self.books_receiver.start()
        self.reviews_receiver.start()

    def shutdown(self):
        logging.info("Graceful shutdown: in progress")
        if self.books_middleware:
            self.books_middleware.shutdown()
            self.books_middleware = None

        if self.reviews_middleware:
            self.reviews_middleware.shutdown()
            self.reviews_middleware = None

        if self.books_receiver:
            self.books_receiver.join()
            self.books_receiver = None

        if self.reviews_receiver:
            self.reviews_receiver.join()
            self.reviews_receiver = None

        logging.info("Graceful shutdown: done")

    def _books_receiver(self):
        logging.info("Initializing Books Middleware")
        self.books_middleware = Middleware(
            input_queues={self.book_input_queue[0]: self.book_input_queue[1]},
            callback=self._add_book,
            eof_callback=self.handle_books_eof,
            output_queues=self.output_queues,
            output_exchanges=self.output_exchanges,
            instance_id=self.instance_id,
            persistence_manager=self.persistence_manager)
        self.books_middleware.start()

    def _reviews_receiver(self):
        logging.info("Initializing Reviews Middleware")
        self.reviews_middleware = Middleware(
            output_queues=self.output_queues,
            output_exchanges=self.output_exchanges,
            instance_id=self.instance_id,
        )
        self.reviews_middleware.add_input_queue(
            f"{self.review_input_queue[0]}_{self.instance_id}",
            exchange=self.review_input_queue[1],
            callback=self._filter_review,
            eof_callback=self.handle_reviews_eof,
            auto_ack=False
        )
        self.reviews_middleware.start()

    def _add_book(self, book: Book):
        if book.client_id not in self.books:
            self.books[book.client_id] = {}
        self.books[book.client_id][book.title] = book.authors
        self.persistence_manager.append(f"{BOOKS_KEY}_{book.client_id}", json.dumps([book.title, book.authors]))
        logging.debug("Received and saved book: %s", book.title)
        if len(self.books[book.client_id]) % 2000 == 0:
            logging.info("[Client %s] Stored books count: %d",
                         book.client_id,  len(self.books))

    def _reset_filter(self, client_id: int):
        self.books.pop(client_id, None)
        self.persistence_manager.delete_keys(f"{BOOKS_KEY}_{client_id}")
        self.eofs.remove(client_id)
        self.persistence_manager.put(EOFS_KEY, json.dumps(list(self.eofs)))
        logging.info("Filter reset for client id %s", client_id)

    def handle_books_eof(self, eof_packet: EOFPacket):
        logging.error(f" [x] Received Books EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)

        self.eofs.add(eof_packet.client_id)
        self.persistence_manager.put(EOFS_KEY, json.dumps(list(self.eofs)))

        if len(eof_packet.ack_instances) == self.cluster_size:
            logging.debug(f" [x] Finished propagating Books EOF: {eof_packet}")
        else:
            self.books_middleware.return_eof(eof_packet)
            logging.debug(f" [x] Propagated Books EOF: {eof_packet}")

    def handle_reviews_eof(self, eof_packet: EOFPacket):
        if eof_packet.client_id not in self.eofs:
            logging.warning("Received reviews EOF before books EOF - requeuing")
            return CallbackAction.REQUEUE
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)
            self._reset_filter(eof_packet.client_id)

        if len(eof_packet.ack_instances) == self.cluster_size:
            self.reviews_middleware.send(EOFPacket(
                eof_packet.client_id,
                eof_packet.packet_id,
            ).encode())
            logging.debug(" [x] Forwarded EOF")
        else:
            self.reviews_middleware.return_eof(eof_packet)

    def _filter_review(self, review: Review):
        if review.client_id not in self.eofs:
            # logging.warning("Received review before books EOF - requeuing")
            # TODO: What happens if the review is requeued AFTER the EOF?
            # TODO: If the review is requeued but the EOF is already queued,
            # the review will be lost -> Eofs should be requeued if this happens
            return CallbackAction.REQUEUE

        if review.book_title not in self.books.get(review.client_id, {}):
            return CallbackAction.ACK

        author = self.books[review.client_id][review.book_title]
        review_and_author = ReviewAndAuthor(
            review.book_title,
            review.score,
            review.text,
            author,
            review.client_id,
            review.packet_id
        )
        self.reviews_middleware.send(review_and_author.encode())
        logging.debug("Filter passed - review for: %s", review.book_title)
        return CallbackAction.ACK

    def _init_state(self):
        # Load books
        for key in self.persistence_manager.get_keys(BOOKS_KEY):
            client_id = int(key.removeprefix(f"{BOOKS_KEY}_"))
            books = self.persistence_manager.get(key).splitlines()
            self.books[client_id] = {}
            for book in books:
                book = json.loads(book)
                self.books[client_id][book[0]] = book[1]
        # Load eofs
        self.eofs = set(json.loads(self.persistence_manager.get(EOFS_KEY) or '[]'))

        logging.info(f"Initialized state with {self.books}, eofs: {self.eofs}")
