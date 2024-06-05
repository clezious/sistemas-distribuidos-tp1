from common.packet import Packet
from common.packet_type import PacketType


class EOFPacket(Packet):
    def __init__(self,
                 ack_instances: list[int] = None,
                 trace_id: str = None):
        super().__init__(trace_id)
        self.ack_instances = ack_instances or []

    @property
    def packet_type(self):
        return PacketType.EOF

    @property
    def payload(self):
        return [self.ack_instances]

    @staticmethod
    def decode(fields: list[str], trace_id: str) -> 'EOFPacket':
        ack_instances = fields[0]
        return EOFPacket(ack_instances, trace_id)

    def __str__(self):
        return self.encode()
