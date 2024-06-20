import docker
import socket
import logging
import time
import random

SOCKET_TIMEOUT = 5


class Docktor:
    def __init__(self,
                 instance_id: int,
                 cluster_size: int,
                 excluded_containers: list[str],
                 project_name: str,
                 kill_probability_percentage: int = 1,
                 sleep_interval: float = 0.07,
                 healthcheck_port: int = 8888):
        self.client = docker.from_env()
        self.client_socket = None
        self.should_stop = False
        self.healthcheck_port = healthcheck_port
        self.instance_name = f"docktor_{instance_id}" if cluster_size > 1 else "docktor"
        self.excluded_containers = excluded_containers
        self.filters = {
            "label": f"com.docker.compose.project={project_name}"
        }
        self.sleep_interval = sleep_interval
        self.kill_probability_percentage = kill_probability_percentage
        socket.setdefaulttimeout(SOCKET_TIMEOUT)

    def start(self):
        logging.info("Docktor started")
        while not self.should_stop:
            self.__check_containers()

    def __check_containers(self):
        for container in self.client.containers.list(all=True, filters=self.filters):
            network_name = container.name.split("-")[1]
            if network_name not in self.excluded_containers and not network_name == self.instance_name:
                dice_throw = random.randint(0, 100)
                if self.kill_probability_percentage > dice_throw and container.status == 'running':
                    logging.info(f"KILLING {network_name} INSTEAD OF CHECKING HEALTHCHECK")
                    container.kill()
                    continue
                try:
                    logging.debug(f"Healthcheck for {network_name}")
                    self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.client_socket.connect((network_name, self.healthcheck_port))
                    self.client_socket.close()
                except (socket.timeout, socket.error) as e:
                    logging.error(e)
                    logging.error(f"Container {network_name} is down. Restarting...")
                    try:
                        # Kill could fail if the container is not running
                        container.kill()
                    except Exception as e:
                        logging.error(e)
                    container.start()
            time.sleep(self.sleep_interval)

    def shutdown(self):
        self.should_stop = True
        self.client_socket.close()
        logging.info("Docktor stopped")
