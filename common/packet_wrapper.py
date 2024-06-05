from common.packet import Packet
from common.packet_decoder import PacketDecoder
from common.packet_type import PacketType


class PacketWrapper(Packet):
    def __init__(self, packet: Packet, client_id: int, message_id: int):
        self.packet = packet
        self.client_id = client_id
        self.message_id = message_id

    @property
    def packet_type(self):
        return PacketType.WRAPPER

    @property
    def payload(self):
        return [self.client_id, self.message_id, self.packet.encode()]

    @property
    def internal_packet(self):
        return self.packet

    @staticmethod
    def decode(fields: list[str]) -> "PacketWrapper":
        client_id = fields[0]
        message_id = fields[1]
        packet = PacketDecoder.decode(fields[2])
        return PacketWrapper(packet, client_id, message_id)
