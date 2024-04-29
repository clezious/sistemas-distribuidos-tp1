from enum import Enum


class BoundaryType(Enum):
    BOOK = 0
    REVIEW = 1

    @staticmethod
    def from_str(s):
        if s == "book":
            return BoundaryType.BOOK
        elif s == "review":
            return BoundaryType.REVIEW
        else:
            raise ValueError("Invalid boundary type: " + s)
