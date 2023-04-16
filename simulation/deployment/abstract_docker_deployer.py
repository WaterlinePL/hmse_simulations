import logging
import os
from abc import ABC, abstractmethod

import docker as docker
from docker.errors import ImageNotFound

logging.basicConfig(level=logging.DEBUG)


class AbstractDockerDeployer(ABC):

    def __init__(self):
        self.docker_client = docker.APIClient()
        try:
            self.docker_client.inspect_image(self.get_docker_image_name())
            logging.info(f"Detected image: {self.get_docker_image_name()}")
        except ImageNotFound:
            logging.info(f"Pulling image: {self.get_docker_image_name()}")
            logging.info(self.docker_client.pull(repository=self.get_docker_image_name(),
                                                 tag=self.get_image_tag()))

        self.workspace_volume = AbstractDockerDeployer._get_workspace_mount(
            self.docker_client.inspect_container(os.environ["HOSTNAME"])['Mounts']
        )

        self.container_name = None
        self.container_name = self.get_container_name()

    @abstractmethod
    def get_container_name(self):
        ...

    @abstractmethod
    def get_docker_image_name(self):
        ...

    @abstractmethod
    def get_docker_repo_name(self):
        ...

    @abstractmethod
    def run_simulation_image(self):
        ...

    def get_image_tag(self):
        return "latest"

    def wait_for_termination(self):
        self.docker_client.wait(self.get_container_name())
        # self.docker_client.remove_container(self.get_container_name())
        logging.info(f"{self.get_container_name()}: calculations completed successfully")

    @staticmethod
    def _get_workspace_mount(mounts):
        socket_path = "/var/run/docker.sock"
        for mount in mounts:
            if socket_path not in mount['Source']:
                return mount['Source']
