import logging
from textblob import TextBlob
from common.book_stats import BookStats
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.review_and_author import ReviewAndAuthor


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
        self.instance_id = instance_id
        self.cluster_size = cluster_size

    def start(self):
        self.middleware.start()

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()

    def _calculate_sentiment(self, review: ReviewAndAuthor):
        sentiment = TextBlob(review.text).sentiment.polarity
        stats = BookStats(
            review.book_title,
            sentiment,
            review.client_id,
            review.packet_id
        )
        self.middleware.send(stats.encode())
        logging.debug("Review %s - Sentiment score: %f",
                      review.book_title, sentiment)

    def _handle_eof(self, eof_packet: EOFPacket):
        if self.instance_id not in eof_packet.ack_instances:
            eof_packet.ack_instances.append(self.instance_id)

        if len(eof_packet.ack_instances) == self.cluster_size:
            self.middleware.send(EOFPacket(
                eof_packet.client_id,
                eof_packet.packet_id,
            ).encode())
            logging.debug("Forwarded EOF")
        else:
            self.middleware.return_eof(eof_packet)
