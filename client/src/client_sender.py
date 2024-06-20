import logging
import signal
import socket
from io import TextIOWrapper

from common.receive_utils import receive_exact

LENGTH_BYTES = 2
CLIENT_ID_BYTES = 2


class ClientSender():
    def __init__(self, file_path: str, ip: str, port: int, client_id=None):
        self.file_path = file_path
        self.ip = ip
        self.port = port
        self.socket = None
        self.client_id = client_id

    def run(self):
        signal.signal(signal.SIGTERM, self.__graceful_shutdown)

        logging.info("Client running")
        self.__connect()
        if self.client_id is None:
            client_id_bytes = receive_exact(self.socket, CLIENT_ID_BYTES)
            self.client_id = int.from_bytes(client_id_bytes, byteorder='big')
        else:
            self.socket.sendall(self.client_id.to_bytes(
                CLIENT_ID_BYTES, byteorder='big'))

        with self.socket:
            with open(self.file_path, encoding="utf-8") as csvfile:
                csvfile.readline()  # Skip header
                self.send_file(csvfile)

        return self.client_id

    def __connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.ip, self.port))
        logging.info("Connected to %s:%d", self.ip, self.port)

    def __graceful_shutdown(self, signum, frame):
        # TODO: Implement graceful shutdown
        raise NotImplementedError("Graceful shutdown not implemented")

    def __send_line(self, line: str):
        encoded_line = line.encode()
        length_bytes = len(encoded_line).to_bytes(
            LENGTH_BYTES, byteorder='big')
        encoded_msg = length_bytes + encoded_line
        self.socket.sendall(encoded_msg)

    def send_file(self, file: TextIOWrapper):
        while line := file.readline():
            try:
                self.__send_line(line.strip())
                logging.debug("Sent line: %s", line.strip())
            except BrokenPipeError:
                logging.info("Connection closed")
                break
