from .hmse_task import hmse_task
from .simulation_tasks import SimulationTasks
from ..simulation_enums import SimulationStageName
from ..simulation_error import SimulationError
from ...hmse_projects.hmse_hydrological_models.processing.data_passing_utils import DataProcessingException
from ...hmse_projects.hmse_hydrological_models.processing.task_logic import data_tasks_logic
from ...hmse_projects.project_metadata import ProjectMetadata
from ...hmse_projects.simulation_mode import SimulationMode


class DataTasks:

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.WEATHER_DATA_TRANSFER)
    def weather_data_to_hydrus(project_metadata: ProjectMetadata) -> None:
        try:
            data_tasks_logic.weather_data_transfer_to_hydrus(
                project_id=project_metadata.project_id,
                start_date=project_metadata.start_date,
                spin_up=project_metadata.spin_up,
                modflow_metadata=project_metadata.modflow_metadata,
                hydrus_to_weather=project_metadata.hydrus_to_weather,
                shapes_to_hydrus=project_metadata.shapes_to_hydrus
            )
        except DataProcessingException as e:
            raise SimulationError(description=str(e))

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_TO_MODFLOW_DATA_PASSING)
    def hydrus_to_modflow(project_metadata: ProjectMetadata) -> None:
        try:
            data_tasks_logic.transfer_data_from_hydrus_to_modflow(
                project_id=project_metadata.project_id,
                shapes_to_hydrus=project_metadata.shapes_to_hydrus,
                is_feedback_loop=project_metadata.simulation_mode == SimulationMode.WITH_FEEDBACK,
                modflow_metadata=project_metadata.modflow_metadata,
                spin_up=project_metadata.spin_up
            )
        except DataProcessingException as e:
            raise SimulationError(description=str(e))

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_TO_HYDRUS_DATA_PASSING)
    def modflow_to_hydrus(project_metadata: ProjectMetadata) -> None:
        data_tasks_logic.transfer_data_from_modflow_to_hydrus(
            project_id=project_metadata.project_id,
            shapes_to_hydrus=project_metadata.shapes_to_hydrus,
            modflow_metadata=project_metadata.modflow_metadata
        )

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_STEADY_STATE)
    def modflow_init_condition_transfer_steady_state(project_metadata: ProjectMetadata) -> None:
        SimulationTasks.modflow_simulation(project_metadata)
        data_tasks_logic.transfer_data_from_modflow_to_hydrus(
            project_id=project_metadata.project_id,
            shapes_to_hydrus=project_metadata.shapes_to_hydrus,
            modflow_metadata=project_metadata.modflow_metadata
        )

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_TRANSIENT)
    def modflow_init_condition_transfer_transient(project_metadata: ProjectMetadata) -> None:
        data_tasks_logic.transfer_data_from_modflow_to_hydrus_init_transient(
            project_id=project_metadata.project_id,
            shapes_to_hydrus=project_metadata.shapes_to_hydrus,
            modflow_metadata=project_metadata.modflow_metadata
        )
