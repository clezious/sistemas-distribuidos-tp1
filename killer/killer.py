import logging
import random
from time import sleep
import docker

EXCLUDED_CONTAINERS = ["rabbitmq", "book_boundary", "review_boundary", "output_boundary", "client"]
FILTERS = {
    "label": "com.docker.compose.project=tp1"
}


class Killer:
    def __init__(self):
        self.docker_client: docker.DockerClient = docker.from_env()
        self.alive_docktors = []

    def russian_roulette(self, kill_probability: float, time_interval: float) -> bool:
        logging.info("Starting russian roulette")
        while True:
            self.__kill_containers(kill_probability, time_interval)

    def nuke(self):
        logging.info("Nuking all containers except the ones in the EXCLUDED_CONTAINERS list and 1 docktor")
        self.__kill_containers(100, 0)
        logging.info("Nuked all containers except the ones in the EXCLUDED_CONTAINERS list and 1 docktor")

    def __kill_containers(self, kill_probability: float, time_interval: float):
        containers = self.docker_client.containers.list(filters=FILTERS)  # List only running containers
        self.alive_docktors = set([container.name for container in containers
                                   if container.name.split("-")[1].startswith("docktor")
                                   and container.status == 'running'])

        for container in containers:
            network_name = container.name.split("-")[1]
            if not self.should_kill(network_name):
                continue

            dice_throw = random.randint(0, 100)
            logging.debug(f"Dice throw: {dice_throw} for {container.name}")

            if kill_probability > dice_throw and container.status == 'running':
                logging.info(f"KILLING {network_name}")
                try:
                    container.kill()
                except Exception as e:
                    logging.error(f"Error while killing container: {e}")

            sleep(time_interval)

    def should_kill(self, service_name: str):
        for excluded in EXCLUDED_CONTAINERS:
            if service_name.startswith(excluded):
                return False

        if service_name.startswith("docktor"):
            if len(self.alive_docktors) == 1:
                logging.info("Only one docktor left, not killing")
                return False

            self.alive_docktors.discard(service_name)
            return False

        return True
