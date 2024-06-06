import logging
import os
from typing import Callable
import pika

from common.packet import Packet
from common.packet_type import PacketType
from common.packet_decoder import PacketDecoder
from common.eof_packet import EOFPacket

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_HEARTBEAT = int(os.getenv('RABBITMQ_PORT', '1200'))


class Middleware:
    def __init__(self,
                 input_queues: dict[str, str] = {},
                 callback: Callable = None,
                 eof_callback: Callable = None,
                 output_queues: list[str] = [],
                 output_exchanges: list[str] = [],
                 n_output_instances: int = None,
                 instance_id: int = None,
                 ):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            RABBITMQ_HOST, RABBITMQ_PORT, heartbeat=RABBITMQ_HEARTBEAT))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.input_queues: dict[str, str] = {}
        self.output_queues = output_queues
        self.output_exchanges = output_exchanges
        self.callback = callback
        self.eof_callback = eof_callback
        self.n_output_instances = n_output_instances
        self.instance_id = instance_id
        self._init_input(input_queues)
        self._init_output()
        self.should_stop = False

    def _init_input(self, input_queues):
        for queue, exchange in input_queues.items():
            suffix = "" if self.instance_id is None else f'_{self.instance_id}'
            self.add_input_queue(
                f'{queue}{suffix}', self.callback, self.eof_callback,
                exchange=exchange)

    def _init_output(self):
        if self.n_output_instances is None:
            for queue in self.output_queues:
                self.channel.queue_declare(queue=queue)
        else:
            for i in range(self.n_output_instances):
                for queue in self.output_queues:
                    self.channel.queue_declare(queue=f'{queue}_{i}')

        for exchange in self.output_exchanges:
            self.channel.exchange_declare(
                exchange=exchange, exchange_type='fanout')

    def start(self):
        logging.info("Middleware started")
        try:
            if self.input_queues:
                self.channel.start_consuming()
        except OSError:
            logging.debug("Middleware shutdown")
        except pika.exceptions.ConnectionClosedByBroker:
            logging.debug("Connection closed")

    def send(self, data: str, instance_id: int = None):
        if not self.should_stop:
            suffix = f"_{instance_id}" if instance_id is not None else ""
            for queue in self.output_queues:
                self.send_to_queue(f'{queue}{suffix}', data)

            for exchange in self.output_exchanges:
                self.channel.basic_publish(
                    exchange=exchange, routing_key='', body=data)
                logging.debug("Sent to exchange %s: %s", exchange, data)

    def send_to_queue(self, queue: str, data: str):
        self.channel.basic_publish(exchange='', routing_key=queue, body=data)
        logging.debug("Sent to queue %s: %s", queue, data)

    def shutdown(self):
        self.should_stop = True
        if self.input_queues:
            self.stop()
        self.connection.close()
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

        if input_queue not in self.input_queues:
            self.input_queues[input_queue] = exchange

    def _callback_wrapper(self,
                          callback: Callable[[Packet], any],
                          eof_callback: Callable[[EOFPacket], any],
                          auto_ack: bool
                          ):

        def wrapper(ch, method, properties, body):
            packet = PacketDecoder.decode(body)

            if packet.packet_type == PacketType.EOF:
                logging.debug("Received EOF packet")
                if eof_callback:
                    eof_callback(packet)
                if not auto_ack:
                    self.ack(method.delivery_tag)
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

    def stop(self):
        self.channel.stop_consuming()
        logging.info("Middleware stopped consuming messages")

    def return_eof(self, eof_packet: EOFPacket):
        data = eof_packet.encode()
        for queue in self.input_queues:
            queue = queue if self.instance_id is None else queue.removesuffix(
                f'{self.instance_id}')+f"{self.instance_id+1}"
            self.channel.basic_publish(
                exchange='', routing_key=queue, body=data)
            logging.debug("Sent to queue %s: %s", queue, data)
