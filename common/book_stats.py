from common.packet_type import PacketType
from common.packet import Packet


class BookStats(Packet):
    def __init__(self,
                 title: str,
                 average_score: float):
        self.title = title
        self.average_score = average_score

    @property
    def packet_type(self):
        return PacketType.BOOK_STATS

    @property
    def payload(self):
        return [self.title,
                self.average_score]

    @staticmethod
    def decode(fields: list[str]):
        title = fields[0]
        average_score = fields[1]
        return BookStats(title, average_score)
    
    def __lt__(self, other: 'BookStats'):
        return self.average_score < other.average_score
    
    def __eq__(self, other: 'BookStats'):
        return self.average_score == other.average_score
    
    def __gt__(self, other: 'BookStats'):
        return self.average_score > other.average_score
