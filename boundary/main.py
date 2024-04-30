from configparser import ConfigParser
import os
import logging
from boundary import Boundary
from boundary_type import BoundaryType


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
            os.getenv('SERVER_LISTEN_BACKLOG',
                      config["DEFAULT"]["SERVER_LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["output_exchange"] = os.getenv('OUTPUT_EXCHANGE')
        config_params["boundary_type"] = os.getenv('BOUNDARY_TYPE')
    except KeyError as e:
        raise e
    except ValueError as e:
        raise e

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])
    boundary_type = BoundaryType.from_str(config_params["boundary_type"])
    boundary = Boundary(config_params["port"], config_params["listen_backlog"],
                        config_params["output_exchange"], boundary_type)
    boundary.run()


if __name__ == "__main__":
    main()
