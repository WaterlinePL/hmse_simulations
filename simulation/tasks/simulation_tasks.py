from abc import ABC, abstractmethod

from ..simulation_enums import SimulationStageName
from ...hmse_projects.project_metadata import ProjectMetadata
from .hmse_task import hmse_task


class SimulationTasks(ABC):

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_SIMULATION)
    def hydrus_simulation(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_SIMULATION)
    def modflow_simulation(project_metadata: ProjectMetadata) -> None:
        ...
