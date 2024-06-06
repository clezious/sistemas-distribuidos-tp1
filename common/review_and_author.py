from common.packet import Packet
from common.packet_type import PacketType


class ReviewAndAuthor(Packet):
    def __init__(self,
                 book_title: str,
                 score: float,
                 text: str,
                 authors: str,
                 client_id: int,
                 packet_id: int):
        super().__init__(client_id, packet_id)
        self.book_title = book_title
        self.score = score
        self.text = text
        self.authors = authors

    @property
    def packet_type(self):
        return PacketType.REVIEW_AND_AUTHOR

    @property
    def payload(self):
        return [self.book_title, self.score, self.text, self.authors]

    @staticmethod
    def decode(fields: list[str]) -> 'ReviewAndAuthor':
        client_id = fields[0]
        packet_id = fields[1]
        title = fields[2]
        score = fields[3]
        text = fields[4]
        authors = fields[5]
        return ReviewAndAuthor(
            title, score, text, authors, client_id, packet_id)
