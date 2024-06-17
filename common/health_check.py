import socket
import logging


class HealthCheck:
    def __init__(self, port: int = 8888, backlog: int = 5):
        self.port = port
        self.backlog = backlog
        self.should_stop = False
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def start(self):
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(self.backlog)
        logging.info("HealthCheck Listening for connections")
        while self.should_stop is False:
            try:
                client_socket, address = self.server_socket.accept()
                logging.info("HealthCheck request from %s", address)
                client_socket.shutdown(socket.SHUT_RDWR)
                client_socket.close()
            except OSError:
                logging.error("HealthCheck socket closed")
                continue

    def shutdown(self):
        self.should_stop = True
        self.server_socket.shutdown(socket.SHUT_RDWR)
        self.server_socket.close()
        logging.info("HealthCheck stopped")
