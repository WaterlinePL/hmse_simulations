import logging
from time import sleep

from .hmse_task import hmse_task
from ..simulation_enums import SimulationStageName
from ...hmse_projects.hmse_hydrological_models.processing.task_logic import configuration_tasks_logic
from ...hmse_projects.project_metadata import ProjectMetadata


class ConfigurationTasks:

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZATION)
    def initialization(project_metadata: ProjectMetadata) -> None:
        logging.info("Initialization mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.SAVE_REFERENCE_HYDRUS_MODELS)
    def save_reference_hydrus_models(project_metadata: ProjectMetadata) -> None:
        logging.info("Hydrus reference models save mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.OUTPUT_EXTRACTION_TO_JSON)
    def output_extraction_to_json(project_metadata: ProjectMetadata) -> None:
        logging.info("Output extraction to JSON mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.CLEANUP)
    def cleanup(project_metadata: ProjectMetadata) -> None:
        logging.info("Cleanup mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZE_NEW_ITERATION_FILES)
    def initialize_new_iteration_files(project_metadata: ProjectMetadata) -> None:
        logging.info("New interation files' initialization mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.CREATE_PER_ZONE_HYDRUS_MODELS)
    def create_per_zone_hydrus_models(project_metadata: ProjectMetadata) -> None:
        logging.info("Per zone hydrus models mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.ITERATION_PRE_CONFIGURATION)
    def iteration_pre_configuration(project_metadata: ProjectMetadata) -> None:
        logging.info("Iteration preconfiguration mock")
        sleep(1)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.FEEDBACK_SAVE_OUTPUT_ITERATION)
    def save_last_iteration(project_metadata: ProjectMetadata) -> None:
        logging.info("Final interation save mock")
        sleep(1)
