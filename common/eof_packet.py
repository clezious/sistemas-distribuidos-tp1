from common.packet import Packet
from common.packet_type import PacketType


class EOFPacket(Packet):
    def __init__(self, ack_instances: list[int] = None):
        self.ack_instances = ack_instances or []

    def packet_type(self):
        return PacketType.EOF

    def payload(self):
        return [self.ack_instances]

    @staticmethod
    def decode(fields: list[str]):
        ack_instances = fields[0]
        return EOFPacket(ack_instances)

    def __str__(self):
        return self.encode()
