from configparser import ConfigParser
import os
import logging
from time import sleep
from src.client import Client
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
        config_params["book_boundary_port"] = int(
            os.getenv('BOOK_BOUNDARY_PORT',
                      config["DEFAULT"]["BOOK_BOUNDARY_PORT"]))
        config_params["book_boundary_ip"] = os.getenv(
            'BOOK_BOUNDARY_IP', config["DEFAULT"]["BOOK_BOUNDARY_IP"])
        config_params["review_boundary_port"] = int(
            os.getenv('REVIEW_BOUNDARY_PORT'))
        config_params["review_boundary_ip"] = os.getenv('REVIEW_BOUNDARY_IP')
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise e
    except ValueError as e:
        raise e

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])
    book_client = Client(
        "../datasets/books_data.csv",
        config_params["book_boundary_ip"],
        config_params["book_boundary_port"])
    book_client.run()
    logging.info("Sent all books")

    review_client = Client(
        "../datasets/books_rating.csv",
        config_params["review_boundary_ip"],
        config_params["review_boundary_port"])
    review_client.run()
    logging.info("Sent all reviews")

    # TODO: Fix bug where the program wont finish sending the files
    sleep(10)


if __name__ == "__main__":
    main()
