import logging
from time import sleep

from .hmse_task import hmse_task
from ..simulation_enums import SimulationStageName
from ...hmse_projects.project_metadata import ProjectMetadata


class DataTasks:

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.WEATHER_DATA_TRANSFER)
    def weather_data_to_hydrus(project_metadata: ProjectMetadata) -> None:
        logging.info("Weather data transfer mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_TO_MODFLOW_DATA_PASSING)
    def hydrus_to_modflow(project_metadata: ProjectMetadata) -> None:
        logging.info("Hydrus -> Modflow transfer mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_TO_HYDRUS_DATA_PASSING)
    def modflow_to_hydrus(project_metadata: ProjectMetadata) -> None:
        logging.info("Modflow -> Hydrus transfer mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_STEADY_STATE)
    def modflow_init_condition_transfer_steady_state(project_metadata: ProjectMetadata) -> None:
        logging.info("Hydrus mock initialization using steady state Modflow 1st step")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_TRANSIENT)
    def modflow_init_condition_transfer_transient(project_metadata: ProjectMetadata) -> None:
        logging.info("Hydrus mock initialization using transient Modflow 1st step")
        sleep(1)
