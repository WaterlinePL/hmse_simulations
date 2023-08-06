from .hmse_task import hmse_task
from ..airflow.airflow_name_converter import convert_hmse_task_to_airflow_task_name
from ..airflow.airflow_simulation_service import airflow_service
from ..simulation_enums import SimulationStageName
from ...hmse_projects.project_dao import project_dao
from ...hmse_projects.project_metadata import ProjectMetadata


class ConfigurationTasks:

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZATION)
    def initialization(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_mapped_job(dag_run_id=kwargs["dag_run_id"],
                                           stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.SAVE_REFERENCE_HYDRUS_MODELS)
    def save_reference_hydrus_models(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.OUTPUT_UPLOAD)
    def output_extraction_to_json(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.CLEANUP)
    def cleanup(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])
        project_metadata.finished = True
        project_dao.save_or_update_metadata(project_metadata)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZE_NEW_ITERATION_FILES)
    def initialize_new_iteration_files(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.CREATE_PER_ZONE_HYDRUS_MODELS)
    def create_per_zone_hydrus_models(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.ITERATION_PRE_CONFIGURATION)
    def iteration_pre_configuration(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.FEEDBACK_SAVE_OUTPUT_ITERATION)
    def save_last_iteration(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])
