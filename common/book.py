
class Book:
    def __init__(self, csv_row: str):
        self.csv_row = csv_row
        fields = csv_row.split(',')
        self.title = fields[0].strip()
        self.author = fields[1].strip()
        self.year = fields[2].strip()
        self.category = fields[3].strip()

    def __str__(self):
        return f"{self.title}, {self.author}, {self.year}, {self.category}"

    def get(self, field):
        if field == 'title':
            return self.title
        if field == 'author':
            return self.author
        if field == 'year':
            return self.year
        if field == 'category':
            return self.category
        return None
