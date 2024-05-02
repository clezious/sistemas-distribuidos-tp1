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
        self._generate_routers()
        self._generate_review_stats_service()
        self._generate_sentiment_analyzer()
        self._generate_sentiment_aggregator()
        return self.config

    def _generate_routers(self):
        self._generate_router(
            "book_router_by_author",
            "authors",
            self.config_params["book_router_by_author"],
            self.config_params["author_decades_counter"],
            {"book_router_by_author": "books"},
            ["books_by_authors"]
        )
        self._generate_router(
            "fiction_book_router_by_title",
            "title",
            self.config_params["fiction_book_router_by_title"],
            self.config_params["review_filter_by_book_category_fiction"],
            {"fiction_books": ""},
            ["fiction_books_by_title"]
        )
        self._generate_router(
            "1990_1999_book_router_by_title",
            "title",
            self.config_params["1990_1999_book_router_by_title"],
            self.config_params["review_filter_by_book_year_1990_1999"],
            {"1990_1999_books": ""},
            ["1990_1999_books_by_title"]
        )
        self._generate_router(
            "fiction_review_router_by_title",
            "book_title",
            self.config_params["fiction_review_router_by_title"],
            self.config_params["review_filter_by_book_category_fiction"],
            {"fiction_reviews_filter_router": "reviews"},
            ["fiction_reviews_by_title"]
        )
        self._generate_router(
            "1990_1999_review_router_by_title",
            "book_title",
            self.config_params["1990_1999_review_router_by_title"],
            self.config_params["review_filter_by_book_year_1990_1999"],
            {"1990_1999_reviews_filter_router": "reviews"},
            ["1990_1999_reviews_by_title"]
        )

        self._generate_router(
            "1990_1999_review_stats_router_by_title",
            "book_title",
            self.config_params["1990_1999_reviews_stats_router_by_title"],
            self.config_params["1990_1999_reviews_stats_router_by_title"],
            {"1990_1999_reviews": ""},
            ["1990_1999_reviews_stats_router_by_title"]
        )

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
                          instances: int = 1):
        for instance_id in range(instances):
            instance_suffix = "" if instances == 1 else f"_{instance_id}"
            service_name_instance = f"{service_name}{instance_suffix}"
            # TODO: make info not the default logging level
            current_environment = ["PYTHONUNBUFFERED=1",
                                   "LOGGING_LEVEL=INFO",
                                   "PYTHONHASHSEED=1234"]
            current_environment.extend(environment)
            current_environment.append(f"INSTANCE_ID={instance_id}")
            current_environment.append(f"CLUSTER_SIZE={instances}")
            if input_queues:
                input_queues_json = json.dumps(
                    input_queues, separators=(',', ':'))
                current_environment.append(f"INPUT_QUEUES={input_queues_json}")

            if output_queues:
                output_queues_json = json.dumps(output_queues)
                current_environment.append(
                    f"OUTPUT_QUEUES={output_queues_json}")

            if output_exchanges is not None:
                output_exchanges_json = json.dumps(output_exchanges)
                current_environment.append(
                    f"OUTPUT_EXCHANGES={output_exchanges_json}")

            config = {
                "image": image,
                "environment": current_environment,
                "networks": networks.copy(),
            }

            if volumes:
                config["volumes"] = volumes.copy()

            if depends_on:
                config["depends_on"] = depends_on.copy()

            self.config["services"][service_name_instance] = config

    def _generate_book_filters_by_category_computers(self):
        instances = self.config_params["book_filter_by_category_computers"]
        self._generate_service(
            "book_filter_by_category_computers",
            "book_filter:latest",
            ['FILTER_BY_FIELD="categories"', 'FILTER_BY_VALUES=["Computers"]'],
            ["test_net"],
            input_queues={"books_filter_by_category_computers": "books"},
            output_queues=["computers_books"],
            output_exchanges=[],
            instances=instances
        )

    def _generate_book_filters_by_category_fiction(self):
        instances = self.config_params["book_filter_by_category_fiction"]
        self._generate_service(
            "book_filter_by_category_fiction",
            "book_filter:latest",
            ['FILTER_BY_FIELD="categories"', 'FILTER_BY_VALUES=["Fiction"]'],
            ["test_net"],
            input_queues={"books_filter_by_category_fiction": "books"},
            output_queues=["fiction_books"],
            output_exchanges=[],
            instances=instances
        )

    def _generate_book_filters_by_year_2000_2023(self):
        instances = self.config_params["book_filter_by_year_2000_2023"]
        self._generate_service(
            "book_filter_by_year_2000_2023",
            "book_filter:latest",
            ['FILTER_BY_FIELD="year"',
             'FILTER_BY_VALUES=[2000,2023]'],
            ["test_net"],
            input_queues={"computers_books": ""},
            output_queues=["2000_2023_computers_books"],
            output_exchanges=[],
            instances=instances
        )

    def _generate_book_filters_by_year_1990_1999(self):
        instances = self.config_params["book_filter_by_year_1990_1999"]
        self._generate_service(
            "book_filter_by_year_1990_1999",
            "book_filter:latest",
            ['FILTER_BY_FIELD="year"',
             'FILTER_BY_VALUES=[1990,1999]'],
            ["test_net"],
            input_queues={"book_filter_by_year_1990_1999": "books"},
            output_queues=["1990_1999_books"],
            output_exchanges=[],
            instances=instances
        )

    def _generate_book_filters_by_title_distributed(self):
        instances = self.config_params["book_filter_by_title_distributed"]
        self._generate_service(
            "book_filter_by_title_distributed",
            "book_filter:latest",
            ['FILTER_BY_FIELD="title"', 'FILTER_BY_VALUES=["Distributed"]'],
            ["test_net"],
            input_queues={"2000_2023_computers_books": ""},
            output_queues=["query1_result"],
            output_exchanges=[],
            instances=instances
        )

    def _generate_author_decades_counters(self):
        instances = self.config_params["author_decades_counter"]
        self._generate_service(
            "author_decades_counter",
            "author_decades_counter:latest",
            [],
            ["test_net"],
            input_queues={"books_by_authors": ""},
            output_queues=["query2_result"],
            output_exchanges=[],
            instances=instances
        )

    def _generate_review_filters_by_book_year_1990_1999(self):
        instances = self.config_params["review_filter_by_book_year_1990_1999"]
        self._generate_service(
            "review_filter_by_book_year_1990_1999",
            "review_filter:latest",
            ['BOOK_INPUT_QUEUE=["1990_1999_books_by_title",""]',
             'REVIEW_INPUT_QUEUE=["1990_1999_reviews_by_title",""]'],
            ["test_net"],
            output_queues=["1990_1999_reviews"],
            output_exchanges=[],
            instances=instances
        )

    def _generate_review_filters_by_book_category_fiction(self):
        instances = self.config_params[
            "review_filter_by_book_category_fiction"]
        self._generate_service(
            "review_filter_by_book_category_fiction",
            "review_filter:latest",
            ['BOOK_INPUT_QUEUE=["fiction_books_by_title",""]',
             'REVIEW_INPUT_QUEUE=["fiction_reviews_by_title",""]'],
            ["test_net"],
            output_queues=["fiction_reviews"],
            output_exchanges=[],
            instances=instances
        )

    def _generate_router(self,
                         name: str,
                         field_to_hash: str,
                         instances: int,
                         target_instances: str,
                         input_queues: dict[str, str],
                         output_queues: list[str]):
        self._generate_service(
            name,
            "router:latest",
            [f'HASH_BY_FIELD={field_to_hash}',
             f'N_INSTANCES={target_instances}'],
            ["test_net"],
            input_queues=input_queues,
            output_queues=output_queues,
            output_exchanges=[],
            instances=instances
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

    def _generate_review_stats_service(self):
        instances = self.config_params["review_stats_service"]
        self._generate_service(
            "review_stats_service",
            "review_stats_service:latest",
            ['REQUIRED_REVIEWS_BOOKS_OUTPUT_QUEUE="query3_result"',
             'TOP_BOOKS_OUTPUT_QUEUE="top_10_books"'],
            ["test_net"],
            input_queues={"1990_1999_reviews_stats_router_by_title": ""},
            instances=instances
        )

    def _generate_sentiment_analyzer(self):
        instances = self.config_params["fiction_review_sentiment_analyzer"]
        self._generate_service(
            "fiction_review_sentiment_analyzer",
            "sentiment_analyzer:latest",
            [],
            ["test_net"],
            input_queues={"fiction_reviews": ""},
            output_queues=["fiction_reviews_sentiment_scores"],
            instances=instances
        )

    def _generate_sentiment_aggregator(self):
        self._generate_service(
            "fiction_review_sentiment_aggregator",
            "sentiment_aggregator:latest",
            [],
            ["test_net"],
            input_queues={"fiction_reviews_sentiment_scores": ""},
            output_queues=["query5_result"],
        )
