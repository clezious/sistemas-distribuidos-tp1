import json
import logging

from common.authors import Authors
from common.book import Book
from common.book_stats import BookStats
from common.eof_packet import EOFPacket
from common.packet_type import PacketType
from common.review import Review
from common.review_and_author import ReviewAndAuthor
from common.packet import Packet


class PacketDecoder():
    @staticmethod
    def decode(body: str) -> 'Packet':
        fields = json.loads(body)
        trace_id = fields[0]
        packet_type = PacketType(fields[1])
        packet_payload = fields[2]
        if packet_type == PacketType.BOOK:
            return Book.decode(packet_payload, trace_id)
        elif packet_type == PacketType.REVIEW:
            return Review.decode(packet_payload, trace_id)
        elif packet_type == PacketType.EOF:
            return EOFPacket.decode(packet_payload, trace_id)
        elif packet_type == PacketType.BOOK_STATS:
            return BookStats.decode(packet_payload, trace_id)
        elif packet_type == PacketType.REVIEW_AND_AUTHOR:
            return ReviewAndAuthor.decode(packet_payload, trace_id)
        elif packet_type == PacketType.AUTHORS:
            return Authors.decode(packet_payload, trace_id)
        else:
            logging.error(f"Tipo de paquete desconocido: {packet_type}")
            raise ValueError("Tipo de paquete desconocido")
