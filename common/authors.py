from common.packet_type import PacketType
from common.packet import Packet


class Authors(Packet):
    def __init__(self,
                 authors: str,
                 client_id: int,
                 packet_id: int):
        super().__init__(client_id, packet_id)
        self.authors = authors

    @property
    def packet_type(self):
        return PacketType.AUTHORS

    @property
    def payload(self):
        return [self.authors]

    @staticmethod
    def decode(fields: list[str], client_id: int, packet_id: int) -> 'Authors':
        authors = fields[0]
        return Authors(authors, client_id, packet_id)
