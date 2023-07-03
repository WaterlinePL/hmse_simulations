from abc import ABC, abstractmethod

from .hmse_task import hmse_task
from ..simulation_enums import SimulationStageName
from ...hmse_projects.project_metadata import ProjectMetadata


class ConfigurationTasks(ABC):

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZATION)
    def initialization(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.SAVE_REFERENCE_HYDRUS_MODELS)
    def save_reference_hydrus_models(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.OUTPUT_UPLOAD)
    def output_extraction_to_json(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.CLEANUP)
    def cleanup(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZE_NEW_ITERATION_FILES)
    def initialize_new_iteration_files(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.CREATE_PER_ZONE_HYDRUS_MODELS)
    def create_per_zone_hydrus_models(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.ITERATION_PRE_CONFIGURATION)
    def iteration_pre_configuration(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.FEEDBACK_SAVE_OUTPUT_ITERATION)
    def save_last_iteration(project_metadata: ProjectMetadata) -> None:
        ...
