from common.packet_type import PacketType
from common.packet import Packet


class BookStats(Packet):
    def __init__(self,
                 title: str,
                 score: float,
                 client_id: int,
                 packet_id: int):
        super().__init__(client_id, packet_id)
        self.title = title
        self.score = score

    @property
    def packet_type(self):
        return PacketType.BOOK_STATS

    @property
    def payload(self):
        return [self.title,
                self.score]

    @staticmethod
    def decode(
            fields: list[str],
            client_id: int, packet_id: int) -> 'BookStats':
        title = fields[0]
        score = fields[1]
        return BookStats(title, score, client_id, packet_id)

    def __lt__(self, other: 'BookStats'):
        return self.score < other.score

    def __eq__(self, other: 'BookStats'):
        return self.score == other.score

    def __gt__(self, other: 'BookStats'):
        return self.score > other.score
