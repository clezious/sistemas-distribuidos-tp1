import json
import csv
import re

from common.packet_type import PacketType
from common.packet import Packet

YEAR_REGEX = re.compile('[^\d]*(\d{4})[^\d]*')


class Book(Packet):
    def __init__(self,
                 title: str,
                 description: str,
                 authors: str,
                 publisher: str,
                 year: int,
                 categories: str,
                 client_id: int,
                 packet_id: int):
        super().__init__(client_id, packet_id)
        self.title = title
        self.description = description
        self.authors = authors
        self.publisher = publisher
        self.year = year
        self.categories = categories

    @staticmethod
    def from_csv_row(csv_row: str, client_id: int, packet_id: int) -> 'Book':
        # Title,description,authors,image,previewLink,publisher,publishedDate,infoLink,categories,ratingsCount
        fields = list(csv.reader([csv_row]))[0]
        title = fields[0].strip()
        description = fields[1].strip()
        authors = fields[2].strip()  # Book.extract_array(fields[2].strip())
        publisher = fields[5].strip()
        year = Book.extract_year(fields[6].strip())
        categories = fields[8].strip()  # Book.extract_array(fields[8].strip())
        for field in [title, authors, year, categories]:
            if not field:
                return None

        return Book(
            title,
            description,
            authors,
            publisher,
            year,
            categories,
            client_id,
            packet_id)

    @property
    def packet_type(self):
        return PacketType.BOOK

    @property
    def payload(self):
        return [self.title,
                self.description,
                self.authors,
                self.publisher,
                self.year,
                self.categories]

    @staticmethod
    def extract_year(x: str):
        if x:
            result = YEAR_REGEX.search(x)
            return int(result.group(1)) if result else None
        return None

    @staticmethod
    def extract_array(x: str):
        if not x:
            return []
        try:
            return json.loads(x.replace("'", '"'))
        except json.JSONDecodeError:
            return []

    @staticmethod
    def decode(fields: list[str]) -> 'Book':
        client_id = fields[0]
        message_id = fields[1]
        title = fields[2]
        description = fields[3]
        authors = fields[4]
        publisher = fields[5]
        year = fields[6]
        categories = fields[7]
        return Book(
            title,
            description,
            authors,
            publisher,
            year,
            categories,
            client_id,
            message_id)

    def __str__(self):
        return self.encode()
