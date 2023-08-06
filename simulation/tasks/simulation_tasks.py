from .hmse_task import hmse_task
from ..airflow.airflow_simulation_service import airflow_service
from ..simulation_enums import SimulationStageName
from ...hmse_projects.project_metadata import ProjectMetadata


class SimulationTasks:

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_SIMULATION)
    def hydrus_simulation(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_mapped_job(dag_run_id=kwargs["dag_run_id"],
                                           stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_SIMULATION_WARMUP)
    def hydrus_simulation_warmup(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_mapped_job(dag_run_id=kwargs["dag_run_id"],
                                           stage_name=kwargs["stage_name"])

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_SIMULATION)
    def modflow_simulation(project_metadata: ProjectMetadata, **kwargs) -> None:
        airflow_service.monitor_mapped_job(dag_run_id=kwargs["dag_run_id"],
                                           stage_name=kwargs["stage_name"])
