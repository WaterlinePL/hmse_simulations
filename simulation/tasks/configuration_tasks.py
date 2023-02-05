from abc import ABC, abstractmethod

from ...hmse_projects.project_metadata import ProjectMetadata
from .hmse_task import hmse_task


class ConfigurationTasks(ABC):

    @staticmethod
    @abstractmethod
    @hmse_task
    def initialization(self) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task
    def output_extraction_to_json(self) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task
    def cleanup(self) -> None:
        ...
