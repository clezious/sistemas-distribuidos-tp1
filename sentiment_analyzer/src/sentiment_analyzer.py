import logging
from textblob import TextBlob
from common.book_stats import BookStats
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.review_and_author import ReviewAndAuthor

REQUIRED_TOTAL_REVIEWS = 500
TOP_BOOKS = 10


class SentimentAnalyzer:
    def __init__(self,
                 input_queues: dict[str, str],
                 output_queues: list[str],
                 instance_id: int,
                 cluster_size: int):
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=output_queues,
            callback=self._calculate_sentiment,
            eof_callback=self._handle_eof,
        )
        self.sentiment_scores: dict[str, dict[str, str]] = {}
        self.instance_id = instance_id
        self.cluster_size = cluster_size

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()

    def _calculate_sentiment(self, review: ReviewAndAuthor):
        sentiment = TextBlob(review.text).sentiment.polarity
        if review.book_title not in self.sentiment_scores:
            self.sentiment_scores[review.book_title] = {
                "total_sentiment": 0,
                "total_reviews": 0
            }

        self.sentiment_scores[review.book_title]["total_sentiment"] += sentiment
        self.sentiment_scores[review.book_title]["total_reviews"] += 1
        logging.debug("Review %s - Sentiment score: %f",
                      review.book_title, sentiment)

    def _send_scores(self):
        for book_title, stats in self.sentiment_scores.items():
            average_score = stats["total_sentiment"] / stats["total_reviews"]
            stats = BookStats(book_title, average_score)
            self.middleware.send(stats.encode())
            logging.debug("Sent sentiment score for %s", book_title)

    def _handle_eof(self, eof_packet: EOFPacket):
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)
            self._send_scores()  # TODO: Check this
            self.sentiment_scores = {}

        if len(eof_packet.ack_instances) == self.cluster_size:
            self.middleware.send(EOFPacket().encode())
            logging.info("Forwarded EOF")
        else:
            self.middleware.return_eof(eof_packet)
