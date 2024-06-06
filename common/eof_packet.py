from common.packet import Packet
from common.packet_type import PacketType


class EOFPacket(Packet):
    def __init__(self,
                 client_id: int,
                 packet_id: int,
                 ack_instances: list[int] = None
                 ):
        super().__init__(client_id, packet_id)
        self.ack_instances = ack_instances or []

    @property
    def packet_type(self):
        return PacketType.EOF

    @property
    def payload(self):
        return [self.ack_instances]

    @staticmethod
    def decode(fields: list[str]) -> 'EOFPacket':
        client_id = fields[0]
        packet_id = fields[1]
        ack_instances = fields[2]
        return EOFPacket(ack_instances, client_id, packet_id)

    def __str__(self):
        return self.encode()
