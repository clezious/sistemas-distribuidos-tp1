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
                 authors: list[str],
                 publisher: str,
                 year: int,
                 categories: list):
        self.title = title
        self.description = description
        self.authors = authors
        self.publisher = publisher
        self.year = year
        self.categories = categories

    @staticmethod
    def from_csv_row(csv_row: str):
        # Title,description,authors,image,previewLink,publisher,publishedDate,infoLink,categories,ratingsCount
        fields = list(csv.reader([csv_row]))[0]
        title = fields[0].strip()
        description = fields[1].strip()
        authors = fields[2].strip().split(',')
        publisher = fields[5].strip()
        year = Book.extract_year(fields[6].strip())
        categories = Book.extract_categories(fields[8].strip())
        return Book(title, description, authors, publisher, year, categories)

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
    def extract_categories(x: str):
        if not x:
            return []
        try:
            return json.loads(x.replace("'", '"'))
        except json.JSONDecodeError:
            return []

    @staticmethod
    def decode(fields: list[str]):
        title = fields[0]
        description = fields[1]
        authors = fields[2]
        publisher = fields[3]
        year = fields[4]
        categories = fields[5]
        return Book(title, description, authors, publisher, year, categories)

    def __str__(self):
        return self.encode()

    def filter_by(self, field: str, values: list):
        if field == 'title':
            for str in values:
                if str.upper() in self.title.upper():
                    return True

        if field == 'authors':
            for author in self.authors:
                if author in values:
                    return True

        if field == 'year' and self.year is not None:
            if self.year >= values[0] and self.year <= values[1]:
                return True

        if field == 'categories' and self.categories is not None:
            for category in self.categories:
                if category in values:
                    return True

        return False


b = Book('title', 'description', ['author1', 'author2'], 'publisher', 2021, [
         'category1', 'category2'])
