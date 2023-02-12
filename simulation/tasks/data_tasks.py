from abc import ABC, abstractmethod

from ..simulation_enums import SimulationStageName
from ...hmse_projects.project_metadata import ProjectMetadata
from .hmse_task import hmse_task


class DataTasks(ABC):

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.WEATHER_DATA_TRANSFER)
    def weather_data_to_hydrus(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.DATA_PASSING)
    def hydrus_to_modflow(project_metadata: ProjectMetadata) -> None:
        ...
