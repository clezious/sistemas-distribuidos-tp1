import json
from common.packet import Packet
from common.packet_decoder import PacketDecoder
from common.packet_type import PacketType

LENGTH_BYTES = 2


class ResultPacket(Packet):
    def __init__(self, query: int, result: Packet):
        super().__init__(result.client_id, result.packet_id)
        self.query = query
        self.result = result

    @property
    def packet_type(self) -> PacketType:
        return PacketType.RESULT

    @property
    def payload(self) -> list:
        return [self.query, self.result.encode()]

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
        return f"ResultPacket(query={self.query}, result={self.result})"
