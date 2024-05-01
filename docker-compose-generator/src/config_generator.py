import json


class ConfigGenerator:
    def __init__(self, config_params):
        self.config_params = config_params
        self.config = {
            "name": "tp1",
            "services": {},
            "networks": {
                "test_net": {
                    "ipam": {
                        "driver": "default",
                        "config": [
                            {
                                "subnet": "172.25.125.0/24"
                            }
                        ]
                    }
                }
            }
        }

    def generate(self) -> dict:
        self._generate_client()
        self._generate_boundary("book", ["books"])
        self._generate_boundary("review", ["reviews"])
        self._generate_book_filters_by_category_computers()
        self._generate_book_filters_by_category_fiction()
        self._generate_book_filters_by_year_2000_2023()
        self._generate_book_filters_by_year_1990_1999()
        self._generate_book_filters_by_title_distributed()
        self._generate_author_decades_counters()
        self._generate_review_filters_by_book_year_1990_1999()
        self._generate_review_filters_by_book_category_fiction()
        return self.config

    def _generate_service(self,
                          service_name: str,
                          image: str,
                          environment: list[str],
                          networks: list[str],
                          volumes: list[str] = [],
                          depends_on: list[str] = [],
                          input_queues: dict[str, str] = None,
                          output_queues: list[str] = None,
                          output_exchanges: list[str] = None,
                          replicas: int = 1):
        default_environment = ["PYTHONUNBUFFERED=1", "LOGGING_LEVEL=INFO"]
        default_environment.extend(environment)
        if input_queues:
            input_queues = json.dumps(input_queues, separators=(',', ':'))
            default_environment.append(f"INPUT_QUEUES={input_queues}")

        if output_queues:
            output_queues = json.dumps(output_queues)
            default_environment.append(f"OUTPUT_QUEUES={output_queues}")

        if output_exchanges is not None:
            output_exchanges = json.dumps(output_exchanges)
            default_environment.append(
                f"OUTPUT_EXCHANGES={output_exchanges}")

        config = {
            "image": image,
            "environment": default_environment,
            "networks": networks,
        }

        if volumes:
            config["volumes"] = volumes

        if depends_on:
            config["depends_on"] = depends_on

        if replicas > 1:
            config["deploy"] = {
                "mode": "replicated",
                "replicas": replicas
            }

        self.config["services"][service_name] = config

    def _generate_book_filters_by_category_computers(self):
        replicas = self.config_params["book_filter_by_category_computers"]
        self._generate_service(
            "book_filter_by_category_computers",
            "book_filter:latest",
            ['FILTER_BY_FIELD="categories"', 'FILTER_BY_VALUES=["Computers"]'],
            ["test_net"],
            input_queues={"books_filter_by_category_computers": "books"},
            output_queues=["computers_books"],
            output_exchanges=[],
            replicas=replicas
        )

    def _generate_book_filters_by_category_fiction(self):
        replicas = self.config_params["book_filter_by_category_fiction"]
        self._generate_service(
            "book_filter_by_category_fiction",
            "book_filter:latest",
            ['FILTER_BY_FIELD="categories"', 'FILTER_BY_VALUES=["Fiction"]'],
            ["test_net"],
            input_queues={"books_filter_by_category_fiction": "books"},
            output_queues=["fiction_books"],
            output_exchanges=[],
            replicas=replicas
        )

    def _generate_book_filters_by_year_2000_2023(self):
        replicas = self.config_params["book_filter_by_year_2000_2023"]
        self._generate_service(
            "book_filter_by_year_2000_2023",
            "book_filter:latest",
            ['FILTER_BY_FIELD="year"',
             'FILTER_BY_VALUES=[2000,2023]'],
            ["test_net"],
            input_queues={"computers_books": ""},
            output_queues=["2000_2023_computers_books"],
            output_exchanges=[],
            replicas=replicas
        )

    def _generate_book_filters_by_year_1990_1999(self):
        replicas = self.config_params["book_filter_by_year_1990_1999"]
        self._generate_service(
            "book_filter_by_year_1990_1999",
            "book_filter:latest",
            ['FILTER_BY_FIELD="year"',
             'FILTER_BY_VALUES=[1990,1999]'],
            ["test_net"],
            input_queues={"book_filter_by_year_1990_1999": "books"},
            output_queues=["1990_1999_books"],
            output_exchanges=[],
            replicas=replicas
        )

    def _generate_book_filters_by_title_distributed(self):
        replicas = self.config_params["book_filter_by_title_distributed"]
        self._generate_service(
            "book_filter_by_title_distributed",
            "book_filter:latest",
            ['FILTER_BY_FIELD="title"', 'FILTER_BY_VALUES=["Distributed"]'],
            ["test_net"],
            input_queues={"book_filter_by_title_distributed": "books"},
            output_queues=["distributed_books"],
            output_exchanges=[],
            replicas=replicas
        )

    def _generate_author_decades_counters(self):
        replicas = self.config_params["author_decades_counter"]
        self._generate_service(
            "author_decades_counter",
            "author_decades_counter:latest",
            [],
            ["test_net"],
            input_queues={"author_decades_counter": "books"},
            output_queues=["query2_result"],
            output_exchanges=[],
            replicas=replicas
        )

    def _generate_review_filters_by_book_year_1990_1999(self):
        replicas = self.config_params["review_filter_by_book_year_1990_1999"]
        self._generate_service(
            "review_filter_by_book_year_1990_1999",
            "review_filter:latest",
            ['BOOK_INPUT_QUEUE=["books_decade_90",""]',
             'REVIEW_INPUT_QUEUE=["reviews_q","reviews"]'],
            ["test_net"],
            output_queues=["1990_1999_reviews"],
            output_exchanges=[],
            replicas=replicas
        )

    def _generate_review_filters_by_book_category_fiction(self):
        replicas = self.config_params["review_filter_by_book_category_fiction"]
        self._generate_service(
            "review_filter_by_book_category_fiction",
            "review_filter:latest",
            ['BOOK_INPUT_QUEUE=["books_fiction",""]',
             'REVIEW_INPUT_QUEUE=["reviews_q","reviews"]'],
            ["test_net"],
            output_queues=["fiction_reviews"],
            output_exchanges=[],
            replicas=replicas
        )

    def _generate_client(self):
        self._generate_service(
            "client",
            "client:latest",
            ["BOOK_BOUNDARY_PORT=12345",
             "BOOK_BOUNDARY_IP=book_boundary",
             "REVIEW_BOUNDARY_PORT=12345",
             "REVIEW_BOUNDARY_IP=review_boundary"],
            ["test_net"],
            ["./datasets:/datasets"],
            depends_on=["book_boundary", "review_boundary"])

    def _generate_boundary(self,
                           boundary_type: str,
                           output_exchanges: list[str] = []):
        self._generate_service(
            f"{boundary_type}_boundary",
            "boundary:latest",
            [f"BOUNDARY_TYPE={boundary_type}",
             "SERVER_PORT=12345",
             "SERVER_LISTEN_BACKLOG=1"],
            ["test_net"],
            output_exchanges=output_exchanges
        )


g = ConfigGenerator({
    "book_filter_by_category_computers": 1,
    "book_filter_by_category_fiction": 3,
    "book_filter_by_year_2000_2023": 4,
    "book_filter_by_year_1990_1999": 2,
    "book_filter_by_title_distributed": 1,
    "author_decades_counter": 3,
    "review_filter_by_book_year_1990_1999": 1,
    "review_filter_by_book_category_fiction": 6
})

g.generate()
