import logging
from common.book import Book
from common.book_stats import BookStats
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.review import Review
from common.review_and_author import ReviewAndAuthor
from common.persistence_manager import PersistenceManager
import json

REQUIRED_TOTAL_REVIEWS = 500
TOP_BOOKS = 10
REVIEW_STATS_KEY_PREFIX = 'review_stats_'


class ReviewStatsService:
    def __init__(self,
                 input_queues: dict[str, str],
                 required_reviews_books_queue: str,
                 top_books_queue: str,
                 instance_id: int,
                 cluster_size: int):
        self.persistence_manager = PersistenceManager(
            f'../storage/review_stats_service_{instance_id}')
        self.book_reviews: dict[int, dict[str, str]] = {}
        self._init_state()
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=[required_reviews_books_queue, top_books_queue],
            callback=self._save_review,
            eof_callback=self._handle_eof,
            instance_id=instance_id,
            persistence_manager=self.persistence_manager,
        )
        self.required_reviews_books_queue = required_reviews_books_queue
        self.top_books_queue = top_books_queue
        self.instance_id = instance_id
        self.cluster_size = cluster_size

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()

    def _get_top_books(self, reviews: dict[str, dict]):
        top_books = sorted(
            reviews.items(),
            key=lambda r: r[1]["total_rating"] / r[1]["total_reviews"],
            reverse=True
        )[:TOP_BOOKS]
        return top_books

    def _get_books_with_required_reviews(self, client_id):
        filtered_books = filter(
            lambda r: r[1]["total_reviews"] >= REQUIRED_TOTAL_REVIEWS,
            self.book_reviews.get(client_id, {}).items()
        )
        return dict(filtered_books)

    def _send_book_stats(self, book_title: str, stats: dict, client_id: int):
        average_score = stats["total_rating"] / stats["total_reviews"]
        book_stats = BookStats(book_title, average_score,
                               client_id, stats["packet_id"])
        self.middleware.send_to_queue(
            self.top_books_queue, book_stats.encode())

    def _send_top_books(self, client_id: int):
        books_required_reviews = self._get_books_with_required_reviews(
            client_id)
        top_books = self._get_top_books(books_required_reviews)
        for book_title, stats in top_books:
            self._send_book_stats(book_title, stats, client_id)
            logging.info("Sent top book to queue: %s", book_title)

        self.book_reviews[client_id] = {}
        self.persistence_manager.delete_keys(f"{REVIEW_STATS_KEY_PREFIX}{client_id}_")
        logging.info("Reset state for client: %s", client_id)

    def _handle_eof(self, eof_packet: EOFPacket):
        logging.debug(f" [x] Received EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)
            self._send_top_books(eof_packet.client_id)

        if len(eof_packet.ack_instances) == self.cluster_size:
            self.middleware.send(
                EOFPacket(
                    client_id=eof_packet.client_id,
                    packet_id=eof_packet.packet_id).encode())
            logging.debug("Forwarded EOF")
        else:
            self.middleware.return_eof(eof_packet)

    def _update_review_stats(self, review: Review):
        self.book_reviews[review.client_id][review.book_title]["total_reviews"] += 1
        self.book_reviews[review.client_id][review.book_title]["total_rating"] += review.score
        self.book_reviews[review.client_id][review.book_title]["packet_id"] = review.packet_id

    def _save_review(self, review: ReviewAndAuthor):
        client_id = review.client_id
        if client_id not in self.book_reviews:
            self.book_reviews[client_id] = {}
        if review.book_title not in self.book_reviews[client_id]:
            self.book_reviews[client_id][review.book_title] = {
                "total_reviews": 1,
                "total_rating": review.score,
                "authors": review.authors,
                "packet_id": review.packet_id,
            }
        else:
            # Only update state if it is not a duplicate
            # (received and saved but then shutdown and restarted before acking the message)
            if self.book_reviews[client_id][review.book_title]["packet_id"] != review.packet_id:
                self._update_review_stats(review)

        key = f'{REVIEW_STATS_KEY_PREFIX}{client_id}_{review.book_title}'
        self.persistence_manager.put(
            key,
            json.dumps(self.book_reviews[client_id][review.book_title])
        )
        logging.debug("Received and saved review for: %s", review.book_title)

        total_reviews = self.book_reviews[client_id][review.book_title]["total_reviews"]
        if total_reviews == REQUIRED_TOTAL_REVIEWS:
            book = Book(review.book_title, "", review.authors,
                        "", -1, [], client_id, review.packet_id)
            self.middleware.send_to_queue(
                self.required_reviews_books_queue,
                book.encode())
            logging.info(f"Sent book to required reviews queue: {book.title}. Client id: {client_id}")

    def _init_state(self):
        self.book_reviews = {}
        for key in self.persistence_manager.get_keys(REVIEW_STATS_KEY_PREFIX):
            [client_id, book_title] = key.removeprefix(REVIEW_STATS_KEY_PREFIX).split('_', maxsplit=1)
            client_id = int(client_id)
            stats = json.loads(self.persistence_manager.get(key))
            if client_id not in self.book_reviews:
                self.book_reviews[client_id] = {}
            self.book_reviews[client_id][book_title] = stats
        logging.info(f"State initialized with {self.book_reviews}")
