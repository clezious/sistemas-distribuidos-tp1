from configparser import ConfigParser
from io import TextIOWrapper
import os
import socket
import logging

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
        config_params["book_boundary_port"] = int(
            os.getenv('BOOK_BOUNDARY_PORT', config["DEFAULT"]["BOOK_BOUNDARY_PORT"]))
        config_params["book_boundary_ip"] = os.getenv(
            'BOOK_BOUNDARY_IP', config["DEFAULT"]["BOOK_BOUNDARY_IP"])
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise e
    except ValueError as e:
        raise e

    return config_params


def send_line(s: socket.socket, line: str):
    encoded_line = line.encode()
    length_bytes = len(encoded_line).to_bytes(LENGTH_BYTES, byteorder='big')
    encoded_msg = length_bytes + encoded_line
    s.sendall(encoded_msg)


def send_file(s: socket.socket, file: TextIOWrapper):
    for line in file.readlines():
        try:
            send_line(s, line)
            logging.debug("Sent line: %s", line.strip())
        except BrokenPipeError:
            logging.info("Connection closed")
            break


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])
    logging.info("Client started")
    with open('./datasets/books_data.csv', encoding="utf-8") as csvfile:
        csvfile.readline() # Skip header
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((config_params["book_boundary_ip"], config_params["book_boundary_port"]))
            logging.info("Connected to book boundary")
            send_file(s, csvfile)
    logging.info("Sent all books")


if __name__ == "__main__":
    main()
