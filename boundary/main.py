from argparse import ArgumentParser
from configparser import ConfigParser
import os
import logging
from server import Server


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


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])
    destination_queue = parse_args()
    server = Server(config_params["port"], config_params["listen_backlog"], destination_queue)
    server.run()


if __name__ == "__main__":
    main()
