import logging
from time import sleep

from .hmse_task import hmse_task
from ..simulation_enums import SimulationStageName
from ...hmse_projects.project_metadata import ProjectMetadata


class SimulationTasks:

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_SIMULATION)
    def hydrus_simulation(project_metadata: ProjectMetadata) -> None:
        logging.info("Hydrus simulation mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_SIMULATION_WARMUP)
    def hydrus_simulation_warmup(project_metadata: ProjectMetadata) -> None:
        logging.info("Modflow simulation mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_SIMULATION)
    def modflow_simulation(project_metadata: ProjectMetadata) -> None:
        logging.info("Hydrus warmup simulation mock")
        sleep(1)
