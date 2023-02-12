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
