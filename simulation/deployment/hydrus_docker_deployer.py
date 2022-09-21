import logging
import os
import uuid

from docker.errors import APIError

from hmse_simulations.hmse_projects.typing_help import ProjectID, HydrusID
from hmse_simulations.path_formatter import format_path_to_docker
from hmse_simulations.simulation.deployment.abstract_docker_deployer import AbstractDockerDeployer


class HydrusDockerDeployer(AbstractDockerDeployer):

    def __init__(self, project_id: ProjectID, hydrus_id: HydrusID):
        self.hydrus_id = hydrus_id
        self.project_id = project_id
        super().__init__()

    def get_container_name(self):
        return self.container_name or f"{uuid.uuid4().hex}-hydrus-{self.hydrus_id}"

    def run_simulation_image(self):
        workspace_dir = format_path_to_docker(self.workspace_volume)
        path_in_ws = os.path.join(self.project_id, 'simulation', 'hydrus', self.hydrus_id)
        container_data = None

        try:
            container_data = self.docker_client.inspect_container(self.get_container_name())
        except APIError as e:
            if e.status_code != 404:
                print(f"Error: {e}")

        if not container_data:
            logging.info(f"Container {self.get_container_name()} does not exist. Creating it...")
            mount_local_path = os.path.join(workspace_dir, path_in_ws)
            host_config = self.docker_client.create_host_config(binds={mount_local_path: {
                'bind': '/workspace',
                'mode': 'rw'
            }})

            container_data = self.docker_client.create_container(image=self.get_docker_image_name(),
                                                                 host_config=host_config,
                                                                 name=self.get_container_name())
            self.docker_client.start(container_data)

    def get_image_tag(self):
        return "latest"

    def get_docker_repo_name(self):
        return "watermodelling"

    def get_docker_image_name(self):
        return "watermodelling/hydrus-1d-docker"
