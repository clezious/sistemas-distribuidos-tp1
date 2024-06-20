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
    def decode(
            fields: list[str],
            client_id: int, packet_id: int) -> 'EOFPacket':
        ack_instances = fields[0]
        return EOFPacket(client_id, packet_id, ack_instances)

    def __str__(self):
        return self.encode()
