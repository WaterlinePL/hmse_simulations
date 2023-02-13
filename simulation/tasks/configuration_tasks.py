from abc import ABC, abstractmethod

from .hmse_task import hmse_task
from ..simulation_enums import SimulationStageName


class ConfigurationTasks(ABC):

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZATION)
    def initialization(self) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.OUTPUT_EXTRACTION_TO_JSON)
    def output_extraction_to_json(self) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.CLEANUP)
    def cleanup(self) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZE_NEW_ITERATION_FILES)
    def initialize_new_iteration_files(self) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.CREATE_PER_ZONE_HYDRUS_MODELS)
    def create_per_zone_hydrus_models(self) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.ITERATION_PRE_CONFIGURATION)
    def iteration_pre_configuration(self) -> None:
        ...
