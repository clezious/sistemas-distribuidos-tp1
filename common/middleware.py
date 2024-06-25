import logging
import os
from typing import Callable
import pika

from common.packet import Packet
from common.packet_type import PacketType
from common.packet_decoder import PacketDecoder
from common.eof_packet import EOFPacket
from common.persistence_manager import PersistenceManager

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_HEARTBEAT = int(os.getenv('RABBITMQ_HEARTBEAT', '1200'))

PROCESSED_KEY = 'processed'


class CallbackAction:
    ACK = "ack"
    NACK = "nack"
    REQUEUE = "requeue"


class Middleware:
    def __init__(self,
                 input_queues: dict[str, str] = {},
                 callback: Callable = None,
                 eof_callback: Callable = None,
                 output_queues: list[str] = [],
                 output_exchanges: list[str] = [],
                 n_output_instances: int = None,
                 instance_id: int = None,
                 persistence_manager: PersistenceManager = None,
                 ):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            RABBITMQ_HOST, RABBITMQ_PORT, heartbeat=RABBITMQ_HEARTBEAT))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=100)
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
        self.persistence_manager = persistence_manager
        self.state: dict[int, set[int]] = {}
        self.init_state()

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

    def _shutdown(self):
        self.should_stop = True

        if self.input_queues:
            self.stop()

        if self.channel:
            self.channel.close()

        self.connection.close()
        logging.info("Middleware stopped")

    def shutdown(self):
        logging.info("Stopping middleware")
        self.connection.add_callback_threadsafe(self._shutdown)

    def add_input_queue(self,
                        input_queue: str,
                        callback: Callable,
                        eof_callback: Callable = None,
                        exchange: str = "",
                        exchange_type: str = "fanout",
                        auto_ack=False):
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
                          callback: Callable[[Packet], CallbackAction],
                          eof_callback: Callable[[EOFPacket], any],
                          auto_ack: bool
                          ):

        def wrapper(ch, method, properties, body):
            packet = PacketDecoder.decode(body)
            action = CallbackAction.ACK
            if packet.packet_type == PacketType.EOF:
                logging.debug("Received EOF packet")
                if eof_callback:
                    action = eof_callback(packet) or CallbackAction.ACK

                if action == CallbackAction.ACK:
                    self.clear_processed(packet.client_id)
            else:
                if not self.is_duplicate(packet):
                    action = callback(packet) or CallbackAction.ACK
                    if action == CallbackAction.ACK:
                        self.mark_as_processed(packet)

            if not auto_ack:
                if action == CallbackAction.ACK:
                    self.ack(method.delivery_tag)
                elif action == CallbackAction.NACK:
                    self.nack(method.delivery_tag)
                elif action == CallbackAction.REQUEUE:
                    self.send_to_queue(method.routing_key, body)
                    self.ack(method.delivery_tag)
                    logging.debug("Requeued packet to %s", method.routing_key)

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

    def is_duplicate(self, packet: Packet) -> bool:
        if self.persistence_manager:
            processed_ids = self.state.get(packet.client_id, set())
            if packet.packet_id in processed_ids:
                logging.debug(f"Packet {packet.trace_id} is a duplicate!")
                return True
        return False

    def mark_as_processed(self, packet: Packet):
        if self.persistence_manager:
            client_id = packet.client_id
            packet_id = packet.packet_id
            if client_id not in self.state:
                self.state[client_id] = set()
            self.state[client_id].add(packet_id)
            key = f"{PROCESSED_KEY}_{client_id}"
            self.persistence_manager.append(key, str(packet_id))
            logging.debug(f"Marked {packet.trace_id} as processed")

    def clear_processed(self, client_id: int):
        if self.persistence_manager:
            key = f"{PROCESSED_KEY}_{client_id}"
            self.persistence_manager.delete_keys(key)
            self.state.pop(client_id, None)
            logging.debug(f"Cleared processed packets for client {client_id}")

    def init_state(self):
        if self.persistence_manager:
            keys = self.persistence_manager.get_keys(PROCESSED_KEY)
            for (key, secondary_key) in keys:
                client_id = int(key.split('_', maxsplit=1)[1])
                processed_ids = self.persistence_manager.get(key, secondary_key).splitlines()
                self.state[client_id] = set(processed_ids)
            logging.debug(f"Initialized state with {self.state}")
        else:
            logging.debug(
                "No persistence manager, skipping state initialization")
