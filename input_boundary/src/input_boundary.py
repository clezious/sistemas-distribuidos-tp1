import logging
import socket

from .boundary_type import BoundaryType
from common.receive_utils import receive_line
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.book import Book
from common.review import Review
import multiprocessing

MAX_READ_SIZE = 1024
LENGTH_BYTES = 2
QUEUE_SIZE = 10000


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
        self.processes = []
        self.packet_queue = multiprocessing.Queue(maxsize=QUEUE_SIZE)
        self.middleware_sender_process = None
        logging.info(
            "Listening for connections and redirecting to %s", output_exchange)

    def run(self):
        client_id = 0
        self.middleware_sender_process = multiprocessing.Process(
            target=self.__middleware_sender)
        self.middleware_sender_process.start()
        while self.should_stop is False:
            try:
                client_socket, address = self.socket.accept()
                logging.info(
                    "Connection from %s - Assigning client id: %s", address,
                    client_id)
                process = multiprocessing.Process(
                    target=self.__handle_client_connection,
                    args=(client_socket, client_id),
                )
                self.processes.append(process)
                process.start()
                client_id += 1
            except OSError:
                logging.error("Server socket closed")
                continue

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
        self.processes = []
        self.socket = client_socket
        self.middleware_sender_process = None
        packet_id = 0
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

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.should_stop = True
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        if self.middleware:
            self.middleware.shutdown()
        for process in self.processes:
            process.join()
        if self.middleware_sender_process:
            self.middleware_sender_process.join()
