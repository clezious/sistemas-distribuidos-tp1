from common.packet_type import PacketType
from common.packet import Packet


class BookStats(Packet):
    def __init__(self,
                 title: str,
                 score: float,
                 trace_id: str = None):
        super().__init__(trace_id)
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
    def decode(fields: list[str], trace_id: str) -> 'BookStats':
        title = fields[0]
        score = fields[1]
        return BookStats(title, score, trace_id)

    def __lt__(self, other: 'BookStats'):
        return self.score < other.score

    def __eq__(self, other: 'BookStats'):
        return self.score == other.score

    def __gt__(self, other: 'BookStats'):
        return self.score > other.score
