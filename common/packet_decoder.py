import json
import logging

from common.authors import Authors
from common.book import Book
from common.book_stats import BookStats
from common.eof_packet import EOFPacket
from common.packet_type import PacketType
from common.review import Review
from common.review_and_author import ReviewAndAuthor


class PacketDecoder():
    @staticmethod
    def decode(body: str):
        fields = json.loads(body)
        packet_type = PacketType(fields[0])
        packet_payload = fields[1]
        if packet_type == PacketType.BOOK:
            return Book.decode(packet_payload)
        elif packet_type == PacketType.REVIEW:
            return Review.decode(packet_payload)
        elif packet_type == PacketType.EOF:
            return EOFPacket.decode(packet_payload)
        elif packet_type == PacketType.BOOK_STATS:
            return BookStats.decode(packet_payload)
        elif packet_type == PacketType.REVIEW_AND_AUTHOR:
            return ReviewAndAuthor.decode(packet_payload)
        elif packet_type == PacketType.AUTHORS:
            return Authors.decode(packet_payload)
        else:
            logging.error(f"Tipo de paquete desconocido: {packet_type}")
            raise ValueError("Tipo de paquete desconocido")
