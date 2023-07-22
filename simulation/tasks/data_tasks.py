from abc import ABC, abstractmethod

from ..simulation_enums import SimulationStageName
from ..simulation_error import SimulationError
from ...hmse_projects.hmse_hydrological_models.processing.data_passing_utils import DataProcessingException
from ...hmse_projects.project_metadata import ProjectMetadata
from .hmse_task import hmse_task


class DataTasks(ABC):

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.WEATHER_DATA_TRANSFER)
    def weather_data_to_hydrus(project_metadata: ProjectMetadata, **kwargs) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_TO_MODFLOW_DATA_PASSING)
    def hydrus_to_modflow(project_metadata: ProjectMetadata, **kwargs) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_TO_HYDRUS_DATA_PASSING)
    def modflow_to_hydrus(project_metadata: ProjectMetadata, **kwargs) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_STEADY_STATE)
    def modflow_init_condition_transfer_steady_state(project_metadata: ProjectMetadata, **kwargs) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_TRANSIENT)
    def modflow_init_condition_transfer_transient(project_metadata: ProjectMetadata, **kwargs) -> None:
        ...
