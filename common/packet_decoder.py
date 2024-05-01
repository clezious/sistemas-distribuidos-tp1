import json

from common.book import Book
from common.eof_packet import EOFPacket
from common.packet_type import PacketType
from common.review import Review


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
            return EOFPacket()
        else:
            raise ValueError("Tipo de paquete desconocido")
