from configparser import ConfigParser
import logging
import os
from src.client_receiver import ClientReceiver
from src.client_sender import ClientSender
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
        config_params["result_boundary_port"] = int(
            os.getenv('RESULT_BOUNDARY_PORT'))
        config_params["result_boundary_ip"] = os.getenv('RESULT_BOUNDARY_IP')
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
    book_client = ClientSender(
        "../datasets/books_data.csv",
        config_params["book_boundary_ip"],
        config_params["book_boundary_port"])
    book_client.run()
    logging.info("Sent all books")

    review_client = ClientSender(
        "../datasets/empty.csv",
        config_params["review_boundary_ip"],
        config_params["review_boundary_port"])
    review_client.run()
    logging.info("Sent all reviews")

    result_client = ClientReceiver(
        config_params["result_boundary_ip"],
        config_params["result_boundary_port"])
    result_client.run()
    logging.info("Received all results")


if __name__ == "__main__":
    main()
