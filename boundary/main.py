from argparse import ArgumentParser
from configparser import ConfigParser
import os
import socket
import logging

MAX_READ_SIZE = 1024
LENGTH_BYTES = 2

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(
            os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["listen_backlog"] = int(
            os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SERVER_LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise e
    except ValueError as e:
        raise e

    return config_params


def parse_args() -> list[str]:
    parser = ArgumentParser()
    parser.add_argument("destination_queue", type=str,
                        help="The destination queue for the messages")
    return parser.parse_args().destination_queue


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


def handle_client_connection(client_socket: socket.socket):
    while True:
        try:
            data = receive_line(client_socket)
            logging.debug("Received line: %s", data.decode().strip())
            # TODO: Send the data to RabbitMQ
        except ConnectionResetError:
            logging.info("Connection closed by client")
            break


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])
    destination_queue = parse_args()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('', config_params["port"]))
    server_socket.listen(config_params["listen_backlog"])
    logging.info(
        "Listening for connections and redirecting to %s", destination_queue)

    while True:
        client_socket, address = server_socket.accept()
        logging.info("Connection from %s", address)
        with client_socket:
            handle_client_connection(client_socket)


if __name__ == "__main__":
    main()
