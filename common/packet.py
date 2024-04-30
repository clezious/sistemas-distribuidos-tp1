from enum import Enum
import json

from common.book import Book
from common.review import Review


class PacketType(Enum):
    EOF = 0
    BOOK = 1
    REVIEW = 2

class Packet:
    def __init__(self, type: PacketType, payload):
        self.packet_type = type
        self.payload = payload

    def encode(self):
        encoded_payload = self.payload.encode() if self.payload else None
        return json.dumps([self.packet_type.value, encoded_payload])
    
    @staticmethod
    def decode(data):
        fields = json.loads(data)
        packet_type = PacketType(fields[0])
        payload = None
        if packet_type == PacketType.BOOK:
            payload = Book.decode(fields[1])
        elif packet_type == PacketType.REVIEW:
            payload = Review.decode(fields[1])

        return Packet(packet_type, payload)