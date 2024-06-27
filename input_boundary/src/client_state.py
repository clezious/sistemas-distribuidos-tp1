from enum import Enum


class ClientState(Enum):
    SENDING_BOOKS = 0
    SENDING_REVIEWS = 1

    @staticmethod
    def from_str(s):
        if s == "sending_books":
            return ClientState.SENDING_BOOKS
        elif s == "sending_reviews":
            return ClientState.SENDING_REVIEWS
        else:
            raise ValueError("Invalid boundary type: " + s)

    def __str__(self):
        if self == ClientState.SENDING_BOOKS:
            return "sending_books"
        elif self == ClientState.SENDING_REVIEWS:
            return "sending_reviews"
