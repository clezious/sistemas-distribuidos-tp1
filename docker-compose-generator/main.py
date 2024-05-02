import configparser

import yaml
from src.config_generator import ConfigGenerator


def initialize_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    if not config.has_section("CONTAINERS"):
        config.read("docker-compose-generator/config.ini")

    if not config.has_section("CONTAINERS"):
        raise ValueError("Config file not found")

    config_params = {}
    config_params["book_filter_by_category_computers"] = int(
        config["CONTAINERS"]["BOOK_FILTER_BY_CATEGORY_COMPUTERS"])
    config_params["book_filter_by_category_fiction"] = int(
        config["CONTAINERS"]["BOOK_FILTER_BY_CATEGORY_FICTION"])
    config_params["book_filter_by_year_2000_2023"] = int(
        config["CONTAINERS"]["BOOK_FILTER_BY_YEAR_2000_2023"])
    config_params["book_filter_by_year_1990_1999"] = int(
        config["CONTAINERS"]["BOOK_FILTER_BY_YEAR_1990_1999"])
    config_params["book_filter_by_title_distributed"] = int(
        config["CONTAINERS"]["BOOK_FILTER_BY_TITLE_DISTRIBUTED"])
    config_params["author_decades_counter"] = int(
        config["CONTAINERS"]["AUTHOR_DECADES_COUNTER"])
    config_params["review_filter_by_book_year_1990_1999"] = int(
        config["CONTAINERS"]["REVIEW_FILTER_BY_BOOK_YEAR_1990_1999"])
    config_params["review_filter_by_book_category_fiction"] = int(
        config["CONTAINERS"]["REVIEW_FILTER_BY_BOOK_CATEGORY_FICTION"])
    config_params["book_router_by_author"] = int(
        config["CONTAINERS"]["BOOK_ROUTER_BY_AUTHOR"])
    config_params["fiction_book_router_by_title"] = int(
        config["CONTAINERS"]["FICTION_BOOK_ROUTER_BY_TITLE"])
    config_params["1990_1999_book_router_by_title"] = int(
        config["CONTAINERS"]["1990_1999_BOOK_ROUTER_BY_TITLE"])
    config_params["fiction_review_router_by_title"] = int(
        config["CONTAINERS"]["FICTION_REVIEW_ROUTER_BY_TITLE"])
    config_params["1990_1999_review_router_by_title"] = int(
        config["CONTAINERS"]["1990_1999_REVIEW_ROUTER_BY_TITLE"])
    config_params["review_stats_service"] = int(
        config["CONTAINERS"]["REVIEW_STATS_SERVICE"])

    return config_params


def main():
    config_params = initialize_config()
    generator = ConfigGenerator(config_params)
    docker_compose_config = generator.generate()
    with open("docker-compose-gen.yaml", "w") as f:
        yaml.dump(docker_compose_config, f,
                  sort_keys=False, default_flow_style=False)

    print("Generated docker-compose-gen.yaml")


if __name__ == "__main__":
    main()
