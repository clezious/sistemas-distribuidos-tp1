from configparser import ConfigParser
import os
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
        config_params["input_gateway_port"] = int(os.getenv('INPUT_GATEWAY_PORT'))
        config_params["input_gateway_ip"] = os.getenv('INPUT_GATEWAY_IP')
        config_params["output_gateway_port"] = int(os.getenv('OUTPUT_GATEWAY_PORT'))
        config_params["output_gateway_ip"] = os.getenv('OUTPUT_GATEWAY_IP')
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise e
    except ValueError as e:
        raise e

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    client = Client("../datasets/books_data.csv",
                    "../datasets/Books_rating.csv",
                    (config_params["input_gateway_ip"],
                     config_params["input_gateway_port"]),
                    (config_params["output_gateway_ip"],
                     config_params["output_gateway_port"]))
    client.run()


if __name__ == "__main__":
    main()
