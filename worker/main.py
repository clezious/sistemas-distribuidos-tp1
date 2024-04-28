import pika
import time
from common.book import Book
import os
import json

print("starting worker...")
connection = pika.BlockingConnection(
    pika.ConnectionParameters('rabbitmq')
)
channel = connection.channel()

filter_by_field: str = json.loads(os.getenv("FILTER_BY_FIELD")) or ''
filter_by_values: list = json.loads(os.getenv("FILTER_BY_VALUES")) or []
input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []
output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []


def filter_book(ch, method, properties, body):
    book = Book(body.decode())
    print(f" [x] Received {book}")
    if book.get(filter_by_field) in filter_by_values:
        print(" [x] Filter passed. ")
        response = book.csv_row
        for queue in output_queues:
            channel.basic_publish(exchange='',
                                  routing_key=queue,
                                  body=response)
            print(f" [x] Sent to queue {queue}")
        for exchange in output_exchanges:
            channel.basic_publish(exchange=exchange,
                                  routing_key='',
                                  body=response)
            print(f" [x] Sent to exchange {exchange}")

    time.sleep(2)
    print(" [x] Done")


for queue in output_queues:
    channel.queue_declare(queue)

for exchange in output_exchanges:
    channel.exchange_declare(exchange, exchange_type='fanout')

for queue, exchange in input_queues.items():
    print(queue, exchange)
    exchange = exchange or ''
    channel.queue_declare(queue)
    if exchange != '':
        channel.exchange_declare(exchange, exchange_type='fanout')
        channel.queue_bind(exchange=exchange, queue=queue)

    channel.basic_consume(queue=queue,
                          on_message_callback=filter_book,
                          auto_ack=True)
channel.start_consuming()
