import logging
from typing import Callable
import pika

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672


class Middleware:
    def __init__(self,
                 input_queues: dict[str, str] = {},
                 callback: Callable = None,
                 output_queues: list[str] = [],
                 output_exchanges: list[str] = []):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.input_queues = input_queues
        self.output_queues = output_queues
        self.output_exchanges = output_exchanges
        self.callback = callback
        self._init_input()
        self._init_output()

    def _init_input(self):
        for queue, exchange in self.input_queues.items():
            self.channel.queue_declare(queue=queue)
            if exchange:
                self.channel.exchange_declare(
                    exchange=exchange, exchange_type='fanout')
                self.channel.queue_bind(exchange=exchange, queue=queue)
            self.channel.basic_consume(
                queue=queue, on_message_callback=self.callback, auto_ack=True)

    def _init_output(self):
        for queue in self.output_queues:
            self.channel.queue_declare(queue=queue)

        for exchange in self.output_exchanges:
            self.channel.exchange_declare(
                exchange=exchange, exchange_type='fanout')

    def start(self):
        if self.input_queues:
            self.channel.start_consuming()
        logging.info("Middleware started")

    def send(self, data: str):
        for queue in self.output_queues:
            self.channel.basic_publish(
                exchange='', routing_key=queue, body=data)
            logging.debug("Sent to queue %s: %s", queue, data)

        for exchange in self.output_exchanges:
            self.channel.basic_publish(
                exchange=exchange, routing_key='', body=data)
            logging.debug("Sent to exchange %s: %s", exchange, data)

    def shutdown(self):
        if self.input_queues:
            self.channel.stop_consuming()
        self.connection.close()
        self.connection = None
        self.channel = None
        logging.info("Middleware stopped")

    def add_input_queue(self,
                        input_queue: str,
                        callback: Callable,
                        exchange: str = "",
                        exchange_type: str = "fanout",
                        auto_ack=True):
        self.channel.queue_declare(queue=input_queue)
        if exchange:
            self.channel.exchange_declare(
                exchange=exchange, exchange_type=exchange_type)
            self.channel.queue_bind(exchange=exchange, queue=input_queue)

        self.channel.basic_consume(
            queue=input_queue, on_message_callback=callback, auto_ack=auto_ack)
        self.input_queues[input_queue] = exchange

    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def nack(self, delivery_tag):
        self.channel.basic_nack(delivery_tag=delivery_tag)
