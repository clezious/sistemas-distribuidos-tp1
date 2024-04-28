import logging
import signal
import socket
from common.middleware import Middleware

MAX_READ_SIZE = 1024
LENGTH_BYTES = 2


class Server():
    def __init__(self, port: int, backlog: int, output_queue: str):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('', port))
        server_socket.listen(backlog)
        self.server_socket = server_socket
        self.port = port

        self.broker_connection = Middleware(output_queues=[output_queue])
        self.broker_connection.send(
            f"Server started on port {port}".encode())
        
        logging.info(
            "Listening for connections and redirecting to %s", output_queue)
        

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
                data = receive_line(client_socket)
                logging.debug("Received line: %s", data.decode().strip())
                self.broker_connection.send(data)
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
            raise ConnectionResetError("Connection closed")
        data += new_data
    return data


def receive_line(s: socket.socket) -> bytes:
    length_bytes = recieve_exact(s, LENGTH_BYTES)
    length = int.from_bytes(length_bytes, byteorder='big')
    data = recieve_exact(s, length)
    return data
