from .hmse_task import hmse_task
from ..airflow.airflow_simulation_service import airflow_service
from ..simulation_enums import SimulationStageName
from ...hmse_projects.project_metadata import ProjectMetadata


class DataTasks:

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.WEATHER_DATA_TRANSFER)
    def weather_data_to_hydrus(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_TO_MODFLOW_DATA_PASSING)
    def hydrus_to_modflow(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_TO_HYDRUS_DATA_PASSING)
    def modflow_to_hydrus(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_STEADY_STATE)
    def modflow_init_condition_transfer_steady_state(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_TRANSIENT)
    def modflow_init_condition_transfer_transient(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_job(dag_run_id=kwargs["dag_run_id"],
                                    stage_name=kwargs["stage_name"])
