import csv
import json


class Review:
    def __init__(self, book_title: str, score, text):
        self.book_title = book_title
        self.score = score
        self.text = text

    
    @staticmethod
    def from_csv_row(csv_row: str):
        # Id,Title,Price,User_id,profileName,review/helpfulness,review/score,review/time,review/summary,review/text
        fields = list(csv.reader([csv_row]))[0]
        title = fields[1].strip()
        score = fields[6].strip()
        text = fields[9].strip()

        return Review(title, score, text)
    
    def encode(self):
        return json.dumps([self.book_title, self.score, self.text])

    @staticmethod
    def decode(data: str):
        fields = json.loads(data)
        title = fields[0]
        score = fields[1]
        text = fields[2]

        return Review(title, score, text)
    
    def __str__(self):
        return self.encode()
