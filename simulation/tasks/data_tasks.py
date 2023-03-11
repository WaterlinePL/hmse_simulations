from .hmse_task import hmse_task
from .simulation_tasks import SimulationTasks
from ..simulation_enums import SimulationStageName
from ..simulation_error import SimulationError
from hmse_simulations.hmse_projects.hmse_hydrological_models.task_logic.data_tasks_logic import DataProcessingException
from ...hmse_projects.hmse_hydrological_models.hydrus import hydrus_utils
from ...hmse_projects.hmse_hydrological_models.task_logic import data_tasks_logic, data_tasks_logic
from ...hmse_projects.project_metadata import ProjectMetadata
from ...hmse_projects.simulation_mode import SimulationMode


class DataTasks:

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.WEATHER_DATA_TRANSFER)
    def weather_data_to_hydrus(project_metadata: ProjectMetadata) -> None:
        hydrus_to_weather_mapping = {hydrus_id: project_metadata.hydrus_to_weather[hydrus_id]
                                     for hydrus_id in project_metadata.get_used_hydrus_models()
                                     if hydrus_id in project_metadata.hydrus_to_weather}
        try:
            data_tasks_logic.pass_weather_data_to_hydrus(project_id=project_metadata.project_id,
                                                         hydrus_to_weather_mapping=hydrus_to_weather_mapping)
        except DataProcessingException as e:
            raise SimulationError(description=str(e))

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_TO_MODFLOW_DATA_PASSING)
    def hydrus_to_modflow(project_metadata: ProjectMetadata) -> None:
        project_id = project_metadata.project_id
        shapes_to_hydrus = project_metadata.shapes_to_hydrus
        use_compound_ids = project_metadata.simulation_mode == SimulationMode.WITH_FEEDBACK

        model_to_shapes_mapping = hydrus_utils.get_hydrus_mapping_for_transfer_to_modflow(shapes_to_hydrus,
                                                                                          use_compound_ids)
        try:
            data_tasks_logic.recharge_from_hydrus_to_modflow(project_id=project_id,
                                                             modflow_id=project_metadata.modflow_metadata.modflow_id,
                                                             spin_up=project_metadata.spin_up,
                                                             model_to_shapes_mapping=model_to_shapes_mapping)
        except DataProcessingException as e:
            raise SimulationError(description=str(e))

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_TO_HYDRUS_DATA_PASSING)
    def modflow_to_hydrus(project_metadata: ProjectMetadata) -> None:
        project_id = project_metadata.project_id
        modflow_id = project_metadata.modflow_metadata.modflow_id
        for shape_id in project_metadata.shapes:
            plain_hydrus_id = project_metadata.shapes_to_hydrus[shape_id]
            if not isinstance(plain_hydrus_id, str):
                continue
            data_tasks_logic.transfer_water_level_to_hydrus(project_id, plain_hydrus_id, modflow_id, shape_id,
                                                            use_modflow_results=True)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_STEADY_STATE)
    def modflow_init_condition_transfer_steady_state(project_metadata: ProjectMetadata) -> None:
        project_id = project_metadata.project_id
        modflow_id = project_metadata.modflow_metadata.modflow_id
        SimulationTasks.modflow_simulation(project_metadata)
        for shape_id in project_metadata.shapes:
            plain_hydrus_id = project_metadata.shapes_to_hydrus[shape_id]
            if not isinstance(plain_hydrus_id, str):
                continue
            data_tasks_logic.transfer_water_level_to_hydrus(project_id, plain_hydrus_id, modflow_id, shape_id,
                                                            use_modflow_results=True)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_TRANSIENT)
    def modflow_init_condition_transfer_transient(project_metadata: ProjectMetadata) -> None:
        project_id = project_metadata.project_id
        modflow_id = project_metadata.modflow_metadata.modflow_id
        for shape_id in project_metadata.shapes:
            plain_hydrus_id = project_metadata.shapes_to_hydrus[shape_id]
            if not isinstance(plain_hydrus_id, str):
                continue
            data_tasks_logic.transfer_water_level_to_hydrus(project_id, plain_hydrus_id, modflow_id, shape_id,
                                                            use_modflow_results=False)
