from configparser import ConfigParser
import logging
import os
from src.input_boundary import InputBoundary
import signal
from common.logs import initialize_log


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
        config_params["listen_backlog"] = int(os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SERVER_LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["books_exchange"] = os.getenv('BOOKS_EXCHANGE')
        config_params["reviews_exchange"] = os.getenv('REVIEWS_EXCHANGE')
    except KeyError as e:
        raise e
    except ValueError as e:
        raise e

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])
    boundary = InputBoundary(config_params["port"],
                             config_params["listen_backlog"],
                             config_params["books_exchange"],
                             config_params["reviews_exchange"])
    signal.signal(signal.SIGTERM, lambda signum, frame: boundary.shutdown())
    boundary.run()
    logging.info("Input gateway stopped")


if __name__ == "__main__":
    main()
