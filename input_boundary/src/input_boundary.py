import logging
from queue import Queue
import socket
import threading
from concurrent.futures import ThreadPoolExecutor

from .boundary_type import BoundaryType
from common.receive_utils import receive_exact, receive_line
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.book import Book
from common.review import Review

MAX_READ_SIZE = 1024
LENGTH_BYTES = 2
QUEUE_SIZE = 10000
CLIENT_ID_BYTES = 2
MAX_CONCURRENT_CONNECTIONS = 5


class InputBoundary:
    def __init__(
            self, port: int, backlog: int, output_exchange: str,
            boundary_type: BoundaryType):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("", port))
        server_socket.listen(backlog)
        self.socket = server_socket
        self.port = port
        self.boundary_type = boundary_type
        self.should_stop = False
        self.output_exchange = output_exchange
        self.threads = {}
        self.packet_queue = Queue(maxsize=QUEUE_SIZE)
        self.middleware_sender_process = None
        self.middleware = None
        self.client_id = 0
        logging.info(
            "Listening for connections and redirecting to %s", output_exchange)

    def run(self):
        self.middleware_sender_process = threading.Thread(
            target=self.__middleware_sender)
        self.middleware_sender_process.start()
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CONNECTIONS) as executor:
            while self.should_stop is False:
                try:
                    client_id = self.__next_client_id()
                    client_socket, address = self.socket.accept()
                    logging.info(
                        "Connection from %s - Assigning client id: %s", address,
                        client_id)

                    thread = executor.submit(
                        self.__handle_client_connection, client_socket,
                        client_id)

                    self.threads[client_id] = thread
                except OSError:
                    logging.error("Server socket closed")
                    continue

    def __next_client_id(self):
        client_id = None
        if self.boundary_type == BoundaryType.BOOK:
            client_id = self.client_id
            self.client_id += 1

        return client_id

    def __middleware_sender(self):
        logging.info("Middleware sender started")
        self.middleware = Middleware(output_exchanges=[self.output_exchange])
        while self.should_stop is False:
            try:
                packet = self.packet_queue.get(block=True)
                self.middleware.send(packet.encode())
            except OSError:
                logging.error("Middleware closed")
                break

    def __handle_client_connection(
            self, client_socket: socket.socket, client_id: int):
        packet_id = 0

        with client_socket:
            if self.boundary_type == BoundaryType.BOOK:
                client_socket.sendall(client_id.to_bytes(
                    CLIENT_ID_BYTES, byteorder='big'))
            elif self.boundary_type == BoundaryType.REVIEW:
                client_id_bytes = receive_exact(client_socket, CLIENT_ID_BYTES)
                client_id = int.from_bytes(client_id_bytes, byteorder='big')

            while self.should_stop is False:
                try:
                    data = receive_line(client_socket,
                                        LENGTH_BYTES).decode().strip()
                    logging.debug("Received line: %s", data)
                    packet = None
                    if self.boundary_type == BoundaryType.BOOK:
                        packet = Book.from_csv_row(data, client_id, packet_id)
                    elif self.boundary_type == BoundaryType.REVIEW:
                        packet = Review.from_csv_row(data, client_id, packet_id)
                    if packet:
                        self.packet_queue.put(packet)
                        packet_id += 1
                except EOFError:
                    logging.info("EOF reached %s", client_id)
                    eof_packet = EOFPacket(client_id, packet_id)
                    self.packet_queue.put(eof_packet)
                    break
                except ConnectionResetError:
                    logging.info("Connection closed by client")
                    break
                except OSError:
                    logging.error("Socket closed")
                    break

        self.threads.pop(client_id, None)

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.should_stop = True
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        if self.middleware:
            self.middleware.shutdown()
            self.middleware = None
        for thread in self.threads.values():
            thread.result()
        if self.middleware_sender_process:
            self.middleware_sender_process.join()
