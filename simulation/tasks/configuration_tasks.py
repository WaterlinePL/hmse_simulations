from .hmse_task import hmse_task
from ..simulation_enums import SimulationStageName
from ...hmse_projects.hmse_hydrological_models.task_logic import configuration_tasks_logic
from ...hmse_projects.project_metadata import ProjectMetadata


class ConfigurationTasks:

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZATION)
    def initialization(project_metadata: ProjectMetadata) -> None:
        configuration_tasks_logic.local_files_initialization(project_metadata.project_id)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.OUTPUT_EXTRACTION_TO_JSON)
    def output_extraction_to_json(project_metadata: ProjectMetadata) -> None:
        configuration_tasks_logic.extract_output_to_json(
            project_id=project_metadata.project_id,
            modflow_id=project_metadata.modflow_metadata.modflow_id
        )

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.CLEANUP)
    def cleanup(project_metadata: ProjectMetadata) -> None:
        # Empty - we do not want to delete results that should be downloaded
        pass

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZE_NEW_ITERATION_FILES)
    def initialize_new_iteration_files(project_metadata: ProjectMetadata) -> None:
        configuration_tasks_logic.initialize_feedback_iteration(
            project_id=project_metadata.project_id,
            modflow_id=project_metadata.modflow_metadata.modflow_id,
            spin_up=project_metadata.spin_up,
            shapes_to_hydrus=project_metadata.shapes_to_hydrus
        )

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.CREATE_PER_ZONE_HYDRUS_MODELS)
    def create_per_zone_hydrus_models(project_metadata: ProjectMetadata) -> None:
        configuration_tasks_logic.create_hydrus_models_for_zones(
            project_id=project_metadata.project_id,
            shapes_to_hydrus=project_metadata.shapes_to_hydrus
        )

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.ITERATION_PRE_CONFIGURATION)
    def iteration_pre_configuration(project_metadata: ProjectMetadata) -> None:
        configuration_tasks_logic.pre_configure_iteration(project_metadata.project_id)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.FEEDBACK_SAVE_OUTPUT_ITERATION)
    def save_last_iteration(project_metadata: ProjectMetadata) -> None:
        ConfigurationTasks.iteration_pre_configuration(project_metadata)
