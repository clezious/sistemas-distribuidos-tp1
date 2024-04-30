import logging
from common.book import Book
from common.middleware import Middleware
from common.packet import Packet, PacketType


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
        packet = Packet.decode(body)
        if packet.packet_type == PacketType.EOF:
            self.books_finished = True
            logging.info("Received books EOF")
            return
        
        book = packet.payload
        self.books[book.title] = book.authors
        logging.debug("Received and saved book: %s", book.title)
    
    def _reset_filter(self):
        self.books_finished = False
        self.books = {}
        logging.info("Filter reset")

    def _filter_review(self, ch, method, properties, body):
        packet = Packet.decode(body)
        if packet.packet_type == PacketType.EOF:
            logging.info("Received reviews EOF")
            self.middleware.ack(method.delivery_tag)
            self._reset_filter()
            return
        
        if not self.books_finished:
            logging.debug("Received review but didnt get books EOF")
            self.middleware.nack(method.delivery_tag)
            return
        
        review = packet.payload
        if review.book_title in self.books:
            self.middleware.send(body)
            logging.info("Filter passed - review for: %s", review.book_title)

        self.middleware.ack(method.delivery_tag)

