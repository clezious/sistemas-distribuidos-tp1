import logging
import signal
import socket

from .boundary_type import BoundaryType
from common.receive_utils import receive_line
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.book import Book
from common.review import Review

MAX_READ_SIZE = 1024
LENGTH_BYTES = 2


class InputBoundary():
    def __init__(self, port: int, backlog: int, output_exchange: str,
                 boundary_type: BoundaryType):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('', port))
        server_socket.listen(backlog)
        self.server_socket = server_socket
        self.port = port
        self.boundary_type = boundary_type

        self.broker_connection = Middleware(output_exchanges=[output_exchange])
        logging.info(
            "Listening for connections and redirecting to %s", output_exchange)

    def run(self):
        signal.signal(signal.SIGTERM, self.__graceful_shutdown)

        while True:
            client_socket, address = self.server_socket.accept()
            logging.info("Connection from %s", address)
            with client_socket:
                self.__handle_client_connection(client_socket)

    def __handle_client_connection(self, client_socket: socket.socket):
        while True:
            try:
                data = receive_line(
                    client_socket, LENGTH_BYTES).decode().strip()
                logging.debug("Received line: %s", data)
                packet = None
                if self.boundary_type == BoundaryType.BOOK:
                    packet = Book.from_csv_row(data)
                elif self.boundary_type == BoundaryType.REVIEW:
                    packet = Review.from_csv_row(data)
                if packet:
                    self.broker_connection.send(packet.encode())
            except EOFError:
                logging.info("EOF reached")
                eof_packet = EOFPacket()
                self.broker_connection.send(eof_packet.encode())
                break
            except ConnectionResetError:
                logging.info("Connection closed by client")
                break

    def __graceful_shutdown(self, signum, frame):
        # TODO: Implement graceful shutdown
        raise NotImplementedError("Graceful shutdown not implemented")
