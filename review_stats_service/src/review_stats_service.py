import logging
from common.book import Book
from common.book_stats import BookStats
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.review import Review
from common.review_and_author import ReviewAndAuthor

REQUIRED_TOTAL_REVIEWS = 500
TOP_BOOKS = 10


class ReviewStatsService:
    def __init__(self,
                 input_queues: dict[str, str],
                 required_reviews_books_queue: str,
                 top_books_queue: str,
                 instance_id: int,
                 cluster_size: int):
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=[required_reviews_books_queue, top_books_queue],
            callback=self._save_review,
            eof_callback=self._handle_eof,
            instance_id=instance_id
        )
        self.book_reviews = {}
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

    def _get_books_with_required_reviews(self):
        filtered_books = filter(
            lambda r: r[1]["total_reviews"] >= REQUIRED_TOTAL_REVIEWS,
            self.book_reviews.items()
        )
        return dict(filtered_books)

    def _send_book_stats(self, book_title: str, stats: dict):
        average_score = stats["total_rating"] / stats["total_reviews"]
        stats = BookStats(book_title, average_score)
        self.middleware.send_to_queue(
            self.top_books_queue, stats.encode())

    def _send_top_books(self):
        books_required_reviews = self._get_books_with_required_reviews()
        top_books = self._get_top_books(books_required_reviews)
        for book_title, stats in top_books:
            self._send_book_stats(book_title, stats)
            logging.info("Sent top book to queue: %s", book_title)

        self.book_reviews = {}
        logging.info("Service reset")

    def _handle_eof(self, eof_packet: EOFPacket):
        logging.info(f" [x] Received EOF: {eof_packet}")
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)
            self._send_top_books()  # TODO: Check this

        if len(eof_packet.ack_instances) == self.cluster_size:
            self.middleware.send(EOFPacket().encode())
            logging.info("Forwarded EOF")
        else:
            self.middleware.return_eof(eof_packet)

    def _update_review_stats(self, review: Review):
        self.book_reviews[review.book_title]["total_reviews"] += 1
        self.book_reviews[review.book_title]["total_rating"] += review.score

    def _save_review(self, review: ReviewAndAuthor):
        if review.book_title not in self.book_reviews:
            self.book_reviews[review.book_title] = {
                "total_reviews": 0,
                "total_rating": 0,
                "authors": review.authors,
            }

        self._update_review_stats(review)
        logging.info("Received and saved review: %s", review.book_title)

        total_reviews = self.book_reviews[review.book_title]["total_reviews"]
        if total_reviews == REQUIRED_TOTAL_REVIEWS:
            book = Book(review.book_title, "", review.authors, "", -1, [])
            self.middleware.send_to_queue(
                self.required_reviews_books_queue,
                book.encode())
            logging.info("Sent book to required reviews queue: %s", book.title)
