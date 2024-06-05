from common.packet_type import PacketType
from common.packet import Packet


class Authors(Packet):
    def __init__(self,
                 authors: str,
                 trace_id: str = None):
        super().__init__(trace_id)
        self.authors = authors

    @property
    def packet_type(self):
        return PacketType.AUTHORS

    @property
    def payload(self):
        return [self.authors]

    @staticmethod
    def decode(fields: list[str], trace_id: str) -> 'Authors':
        authors = fields[0]
        return Authors(authors, trace_id)
