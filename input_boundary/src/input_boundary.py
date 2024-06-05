import logging
import socket

from common.packet_wrapper import PacketWrapper

from .boundary_type import BoundaryType
from common.receive_utils import receive_line
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.book import Book
from common.review import Review
import multiprocessing

MAX_READ_SIZE = 1024
LENGTH_BYTES = 2


class InputBoundary:
    def __init__(
        self, port: int, backlog: int, output_exchange: str, boundary_type: BoundaryType
    ):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("", port))
        server_socket.listen(backlog)
        self.socket = server_socket
        self.port = port
        self.boundary_type = boundary_type
        self.should_stop = False
        self.middleware = None
        self.output_exchange = output_exchange
        self.processes = []
        self.current_id = [multiprocessing.Lock(), 0]
        logging.info("Listening for connections and redirecting to %s", output_exchange)

    def run(self):
        client_id = 0
        while self.should_stop is False:
            try:
                client_socket, address = self.socket.accept()
                logging.info("Connection from %s", address)
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

    def __handle_client_connection(self, client_socket: socket.socket, client_id: int):
        self.processes = []
        self.socket = client_socket
        self.middleware = Middleware(output_exchanges=[self.output_exchange])
        while self.should_stop is False:
            try:
                data = receive_line(client_socket, LENGTH_BYTES).decode().strip()
                message_id = self.__next_id()
                logging.debug("Received line: %s", data)
                packet = None
                if self.boundary_type == BoundaryType.BOOK:
                    packet = Book.from_csv_row(data)
                elif self.boundary_type == BoundaryType.REVIEW:
                    packet = Review.from_csv_row(data)
                if packet:
                    wrapped_packet = PacketWrapper(packet, client_id, message_id)
                    self.middleware.send(wrapped_packet.encode())
            except EOFError:
                logging.info("EOF reached")
                eof_packet = EOFPacket()
                self.middleware.send(eof_packet.encode())
                break
            except ConnectionResetError:
                logging.info("Connection closed by client")
                break
            except OSError:
                logging.error("Socket closed")
                break

    def __next_id(self):
        with self.current_id[0]:
            self.current_id[1] += 1
            return self.current_id[1]

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.should_stop = True
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        self.middleware.shutdown()
        for process in self.processes:
            process.join()
