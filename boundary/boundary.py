import logging
import signal
import socket
from boundary_type import BoundaryType
from common.middleware import Middleware
from common.book import Book
from common.packet import Packet, PacketType
from common.review import Review

MAX_READ_SIZE = 1024
LENGTH_BYTES = 2


class Boundary():
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
                data = receive_line(client_socket).decode().strip()
                logging.debug("Received line: %s", data)
                payload = None
                packet_type = None
                if self.boundary_type == BoundaryType.BOOK:
                    packet_type = PacketType.BOOK
                    payload = Book.from_csv_row(data)
                elif self.boundary_type == BoundaryType.REVIEW:
                    packet_type = PacketType.REVIEW
                    payload = Review.from_csv_row(data)

                packet = Packet(packet_type, payload)
                self.broker_connection.send(packet.encode())
            except EOFError:
                logging.info("EOF reached")
                eof_packet = Packet(PacketType.EOF, None)
                self.broker_connection.send(eof_packet.encode())
                break
            except ConnectionResetError:
                logging.info("Connection closed by client")
                break

    def __graceful_shutdown(self, signum, frame):
        # TODO: Implement graceful shutdown
        raise NotImplementedError("Graceful shutdown not implemented")


def recieve_exact(s: socket.socket, length: int) -> bytes:
    data = b''
    while len(data) < length:
        bytes_remaining = length - len(data)
        new_data = s.recv(min(MAX_READ_SIZE, bytes_remaining))
        if not new_data:
            raise EOFError("EOF reached while reading data")
        data += new_data
    return data


def receive_line(s: socket.socket) -> bytes:
    length_as_bytes = recieve_exact(s, LENGTH_BYTES)
    length = int.from_bytes(length_as_bytes, byteorder='big')
    data = recieve_exact(s, length)
    return data
