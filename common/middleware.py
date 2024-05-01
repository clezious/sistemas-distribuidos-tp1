import logging
from typing import Callable
import pika

from common.packet import Packet
from common.packet_type import PacketType
from common.packet_decoder import PacketDecoder
from common.eof_packet import EOFPacket

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672


class Middleware:
    def __init__(self,
                 input_queues: dict[str, str] = {},
                 callback: Callable = None,
                 output_queues: list[str] = [],
                 output_exchanges: list[str] = [],
                 n_output_instances: int = 1
                 ):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.input_queues = input_queues
        self.output_queues = output_queues
        self.output_exchanges = output_exchanges
        self.callback = callback
        self.n_output_instances = n_output_instances
        self._init_input()
        self._init_output()

    def _init_input(self):
        for queue, exchange in self.input_queues.items():
            self.add_input_queue(queue, self.callback, exchange=exchange)

    def _init_output(self):
        for i in range(self.n_output_instances):
            suffix = "" if self.n_output_instances == 1 else f'_{i}'
            for queue in self.output_queues:
                self.channel.queue_declare(queue=f'{queue}{suffix}')

        for exchange in self.output_exchanges:
            self.channel.exchange_declare(
                exchange=exchange, exchange_type='fanout')

    def start(self):
        if self.input_queues:
            self.channel.start_consuming()
        logging.info("Middleware started")

    def send(self, data: str, instance_id: int = None):
        suffix = f"_{instance_id}" if instance_id else ""
        for queue in self.output_queues:
            self.channel.basic_publish(
                exchange='', routing_key=f'{queue}{suffix}', body=data)
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
                        eof_callback: Callable = None,
                        exchange: str = "",
                        exchange_type: str = "fanout",
                        auto_ack=True):
        self.channel.queue_declare(queue=input_queue)
        if exchange:
            self.channel.exchange_declare(
                exchange=exchange, exchange_type=exchange_type)
            self.channel.queue_bind(exchange=exchange, queue=input_queue)

        wrapped_callback = self._callback_wrapper(callback,
                                                  eof_callback,
                                                  auto_ack)
        self.channel.basic_consume(
            queue=input_queue,
            on_message_callback=wrapped_callback,
            auto_ack=auto_ack)
        self.input_queues[input_queue] = exchange

    def _callback_wrapper(self,
                          callback: Callable[[Packet], any],
                          eof_callback: Callable[[EOFPacket], any],
                          auto_ack: bool
                          ):

        def wrapper(ch, method, properties, body):
            packet = PacketDecoder.decode(body)
            should_ack = True

            if packet.packet_type() == PacketType.EOF:
                logging.info("Received EOF packet")
                # TODO: Check this
                self.send(packet.encode())
                if eof_callback:
                    eof_callback(packet)
            else:
                # Check if auto ack is on
                should_ack = callback(packet)

            if not auto_ack:
                if should_ack:
                    self.ack(method.delivery_tag)
                else:
                    self.nack(method.delivery_tag)

        return wrapper

    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def nack(self, delivery_tag):
        self.channel.basic_nack(delivery_tag=delivery_tag)
