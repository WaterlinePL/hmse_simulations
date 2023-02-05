from abc import ABC, abstractmethod

from ...hmse_projects.project_metadata import ProjectMetadata
from .hmse_task import hmse_task


class DataTasks(ABC):

    @staticmethod
    @abstractmethod
    @hmse_task
    def weather_data_to_hydrus(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task
    def hydrus_to_modflow(project_metadata: ProjectMetadata) -> None:
        ...
