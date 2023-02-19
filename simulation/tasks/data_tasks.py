from abc import ABC, abstractmethod

from .hmse_task import hmse_task
from ..simulation_enums import SimulationStageName
from ..simulation_error import SimulationError
from ...hmse_projects.hmse_hydrological_models import data_passing
from ...hmse_projects.hmse_hydrological_models.data_passing import DataProcessingException
from ...hmse_projects.project_metadata import ProjectMetadata


class DataTasks(ABC):

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.WEATHER_DATA_TRANSFER)
    def weather_data_to_hydrus(project_metadata: ProjectMetadata) -> None:
        hydrus_to_weather_mapping = {hydrus_id: project_metadata.hydrus_to_weather[hydrus_id]
                                     for hydrus_id in project_metadata.get_used_hydrus_models()
                                     if hydrus_id in project_metadata.hydrus_to_weather}
        try:
            data_passing.pass_weather_data_to_hydrus(project_id=project_metadata.project_id,
                                                     hydrus_to_weather_mapping=hydrus_to_weather_mapping)
        except DataProcessingException as e:
            raise SimulationError(description=str(e))

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_TO_MODFLOW_DATA_PASSING)
    def hydrus_to_modflow(project_metadata: ProjectMetadata) -> None:
        project_id = project_metadata.project_id
        model_to_shapes_mapping = {
            mapping_val: [shape_id for shape_id in project_metadata.shapes_to_hydrus.keys()
                          if project_metadata.shapes_to_hydrus[shape_id] == mapping_val]
            for mapping_val in project_metadata.get_used_shape_mappings()
        }
        try:
            data_passing.recharge_from_hydrus_to_modflow(project_id=project_id,
                                                         modflow_id=project_metadata.modflow_metadata.modflow_id,
                                                         spin_up=project_metadata.spin_up,
                                                         model_to_shapes_mapping=model_to_shapes_mapping)
        except DataProcessingException as e:
            raise SimulationError(description=str(e))

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_TO_HYDRUS_DATA_PASSING)
    def modflow_to_hydrus(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_STEADY_STATE)
    def modflow_init_condition_transfer_steady_state(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_TRANSIENT)
    def modflow_init_condition_transfer_transient(project_metadata: ProjectMetadata) -> None:
        ...
