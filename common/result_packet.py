import json
from common.book import Book
from common.packet import Packet
from common.packet_decoder import PacketDecoder

LENGTH_BYTES = 2


class ResultPacket():
    def __init__(self, query: int, result: Packet):
        self.query = query
        self.result = result

    def encode(self) -> str:
        encoded_res = json.dumps([self.query, self.result.encode()])
        length = len(encoded_res).to_bytes(LENGTH_BYTES, byteorder='big')
        return length + encoded_res.encode()

    @staticmethod
    def decode(data: str) -> 'ResultPacket':
        fields = json.loads(data)
        query = int(fields[0])
        result = PacketDecoder().decode(fields[1])
        return ResultPacket(query, result)

    def __str__(self):
        return self.encode()

b = Book(1, "title", "author", 2021, "genre", "publisher")
r = ResultPacket(1, b)