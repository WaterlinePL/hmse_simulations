from abc import ABC, abstractmethod

from ...hmse_projects.project_metadata import ProjectMetadata
from .hmse_task import hmse_task


class SimulationTasks(ABC):

    @staticmethod
    @abstractmethod
    @hmse_task
    def hydrus_simulation(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task
    def modflow_simulation(project_metadata: ProjectMetadata) -> None:
        ...
