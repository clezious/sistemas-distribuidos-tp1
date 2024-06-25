import logging
import threading
import time
from common.book import Book
from common.eof_packet import EOFPacket
from common.middleware import CallbackAction, Middleware
from common.review import Review
from common.review_and_author import ReviewAndAuthor
from common.persistence_manager import PersistenceManager
import json

BOOKS_KEY = 'books'
EOFS_KEY = 'eofs'
REQUEUE_EOF_KEY = 'should_requeue_eof'
CLEANUP_TIMEOUT = 60 * 20  # 20 minutes


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
        self.should_requeue_eof: set[int] = set()
        self.last_packet_timestamp: dict[int, float] = {}

        self.persistence_manager = PersistenceManager(
            f'../storage/review_filter_{review_input_queue[0]}_{book_input_queue[0]}_{instance_id}')
        self._init_state()

        self.reviews_middleware = None
        self.books_middleware = None
        self.books_receiver = threading.Thread(target=self._books_receiver)
        self.reviews_receiver = threading.Thread(target=self._reviews_receiver)
        self.cleaner = threading.Thread(target=self._cleaner)
        self.should_stop = False
        self.lock = threading.Lock()

    def start(self):
        self.books_receiver.start()
        self.reviews_receiver.start()
        self.cleaner.start()

    def shutdown(self):
        logging.info("Graceful shutdown: in progress")
        self.should_stop = True

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

        if self.cleaner:
            self.cleaner.join()
            self.cleaner = None

        logging.info("Graceful shutdown: done")

    def _cleaner(self):
        while not self.should_stop:
            with self.lock:
                ids_to_remove = [
                    client_id for client_id,
                    last_timestamp in self.last_packet_timestamp.items()
                    if time.time() - last_timestamp > CLEANUP_TIMEOUT]

                for client_id in ids_to_remove:
                    logging.info(
                        "[CLEANER] Cleaning up client id %s due to timeout",
                        client_id)
                    self._reset_filter(client_id)
                    # TODO: Should it send an EOF to the next node?

            time.sleep(CLEANUP_TIMEOUT // 10)

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
        self.persistence_manager.append(
            f"{BOOKS_KEY}_{book.client_id}", json.dumps(
                [book.title, book.authors]))
        logging.debug("Received and saved book: %s", book.title)
        if len(self.books[book.client_id]) % 2000 == 0:
            logging.info("[Client %s] Stored books count: %d",
                         book.client_id,  len(self.books))

    def _reset_filter(self, client_id: int):
        self.books.pop(client_id, None)
        self.persistence_manager.delete_keys(f"{BOOKS_KEY}_{client_id}")
        self.eofs.remove(client_id)
        self.persistence_manager.put(EOFS_KEY, json.dumps(list(self.eofs)))

        if client_id in self.should_requeue_eof:
            self.__remove_should_requeue_eof(client_id)

        self.last_packet_timestamp.pop(client_id, None)

        logging.info("Filter reset for client id %s", client_id)

    def __add_should_requeue_eof(self, client_id: int):
        self.should_requeue_eof.add(client_id)
        self.persistence_manager.put(REQUEUE_EOF_KEY, json.dumps(list(self.should_requeue_eof)))

    def __remove_should_requeue_eof(self, client_id: int):
        self.should_requeue_eof.remove(client_id)
        self.persistence_manager.put(REQUEUE_EOF_KEY, json.dumps(list(self.should_requeue_eof)))

    def handle_books_eof(self, eof_packet: EOFPacket):
        logging.debug(f" [x] Received Books EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)
            with self.lock:
                self.last_packet_timestamp[eof_packet.client_id] = time.time()

        self.eofs.add(eof_packet.client_id)
        self.persistence_manager.put(EOFS_KEY, json.dumps(list(self.eofs)))

        if len(eof_packet.ack_instances) == self.cluster_size:
            logging.debug(f" [x] Finished propagating Books EOF: {eof_packet}")
        else:
            self.books_middleware.return_eof(eof_packet)
            logging.debug(f" [x] Propagated Books EOF: {eof_packet}")

    def handle_reviews_eof(self, eof_packet: EOFPacket):
        if eof_packet.client_id not in self.eofs or eof_packet.client_id in self.should_requeue_eof:
            logging.warning("Received reviews EOF but have to requeue it - requeuing")
            if eof_packet.client_id in self.should_requeue_eof:
                self.__remove_should_requeue_eof(eof_packet.client_id)
            return CallbackAction.REQUEUE

        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)
            with self.lock:
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
            if review.client_id not in self.should_requeue_eof:
                self.__add_should_requeue_eof(review.client_id)
            return CallbackAction.REQUEUE

        with self.lock:
            self.last_packet_timestamp[review.client_id] = time.time()

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
        for (key, secondary_key) in self.persistence_manager.get_keys(BOOKS_KEY):
            client_id = int(key.removeprefix(f"{BOOKS_KEY}_"))
            books = self.persistence_manager.get(
                key, secondary_key).splitlines()
            self.books[client_id] = {}
            for book in books:
                book = json.loads(book)
                self.books[client_id][book[0]] = book[1]

        # Load eofs
        self.eofs = set(json.loads(
            self.persistence_manager.get(EOFS_KEY) or '[]'))

        # Initialize last packet timestamps with current time
        for client_id in self.eofs:
            self.last_packet_timestamp[client_id] = time.time()

        # Load should_requeue_eof
        self.should_requeue_eof = set(json.loads(self.persistence_manager.get(REQUEUE_EOF_KEY) or '[]'))

        logging.info(
            f"Initialized state with {self.books}, eofs: {self.eofs}, should_requeue_eof: {self.should_requeue_eof}")
