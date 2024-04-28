#!/usr/bin/env python
import pika
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq')
)
channel = connection.channel()
channel.exchange_declare(exchange='books', exchange_type='fanout')

books = [
    'The Great Gatsby, F. Scott Fitzgerald, 1925, fiction',
    'War and Peace, Leo Tolstoy, 1869, fiction',
    '1984, George Orwell, 1949, fiction',
    'Ulysses, James Joyce, 1922, fiction',
    'Lolita, Vladimir Nabokov, 1955, fiction',
    'The Catcher in the Rye, J.D. Salinger, 1951, fiction',
    'The Elements of Style, William Strunk Jr., 1918, non fiction',
    'The Art of War, Sun Tzu, 5th century BC, non fiction',
]

for book in books:
    channel.basic_publish(exchange='books',
                          routing_key='',
                          body=book)
    time.sleep(1)

print(" All books sent! ")
connection.close()
