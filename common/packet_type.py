from enum import Enum


class PacketType(Enum):
    EOF = 0
    BOOK = 1
    REVIEW = 2
    BOOK_STATS = 3
    REVIEW_AND_AUTHOR = 4
    RESULT = 5
    AUTHORS = 6
