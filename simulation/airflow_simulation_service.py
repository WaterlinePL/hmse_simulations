import logging
import os

import requests
from requests.auth import HTTPBasicAuth

from hmse_simulations.hmse_projects.project_dao import project_dao
from hmse_simulations.hmse_projects.project_metadata import ProjectMetadata
from hmse_simulations.simulation.simulation_enums import SimulationStageStatus
from hmse_simulations.simulation.simulation_error import SimulationError

AIRFLOW_API_ENDPOINT = os.environ["AIRFLOW_API_ENDPOINT"]
AIRFLOW_USER = os.environ["AIRFLOW_USER"]
AIRFLOW_PASSWORD = os.environ["AIRFLOW_PASSWORD"]
AIRFLOW_SIMULATION_DAG = os.environ["AIRFLOW_SIMULATION_DAG"]


class AirflowSimulationService:
    def __init__(self):
        self.auth = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD)
        self.__init_activate_dags()

    def start_simulation(self, run_id: str, project_metadata: ProjectMetadata):
        req_content = {
            "conf": AirflowSimulationService.__prepare_config_json(project_metadata),
            "dag_run_id": run_id
        }
        resp = requests.post(AirflowSimulationService.__get_endpoint_for_simulation_start(),
                             json=req_content,
                             auth=self.auth)
        if resp.status_code != 200:
            raise SimulationError(description="Simulation failed to start!")

    def get_simulation_stage_status(self, run_id: str, task_id: str) -> SimulationStageStatus:
        resp = requests.get(self.__get_endpoint_for_task(run_id, task_id), auth=self.auth)
        status = resp.json()["state"]
        return AirflowSimulationService.__analyze_single_status(status)

    def get_mapped_stage_status(self, run_id: str, task_id: str) -> SimulationStageStatus:
        mapped_endpoint = f"{self.__get_endpoint_for_task(run_id, task_id)}/listMapped"
        resp = requests.get(mapped_endpoint, auth=self.auth)
        task_instances = resp.json()["task_instances"]
        task_statuses = set(AirflowSimulationService.__analyze_single_status(inst["state"]) for inst in task_instances)
        if SimulationStageStatus.ERROR in task_statuses:
            return SimulationStageStatus.ERROR
        elif SimulationStageStatus.RUNNING in task_statuses:
            return SimulationStageStatus.RUNNING
        elif SimulationStageStatus.PENDING in task_statuses:
            return SimulationStageStatus.PENDING
        return SimulationStageStatus.SUCCESS

    def __init_activate_dags(self):
        dag_pattern = "hmse_"
        req_content = {
            "is_paused": False
        }
        resp = requests.patch(AirflowSimulationService.__get_endpoint_for_dags_state_update(),
                              params={
                                  "dag_id_pattern": dag_pattern,
                              },
                              json=req_content,
                              auth=self.auth)

        if resp.status_code != 200:
            raise RuntimeError("DAG initialization failed!")

        initialized_dags = [dag["dag_id"] for dag in resp.json()["dags"]]
        logging.info(f"Successfully initialized DAGs: {initialized_dags}")

    @staticmethod
    def __get_endpoint_for_task(run_id: str, task_id: str) -> str:
        return (f"http://{AIRFLOW_API_ENDPOINT}/dags/{AIRFLOW_SIMULATION_DAG}/dagRuns/{run_id}/"
                f"taskInstances/{task_id}")

    @staticmethod
    def __analyze_single_status(airflow_task_status: str):
        if airflow_task_status in ("success", "skipped"):
            return SimulationStageStatus.SUCCESS
        elif airflow_task_status in ("failed", "upstream_failed"):
            return SimulationStageStatus.ERROR
        elif airflow_task_status == "running":
            return SimulationStageStatus.RUNNING
        return SimulationStageStatus.PENDING

    @staticmethod
    def __get_endpoint_for_simulation_start():
        return f"http://{AIRFLOW_API_ENDPOINT}/dags/{AIRFLOW_SIMULATION_DAG}/dagRuns"

    @staticmethod
    def __get_endpoint_for_dags_state_update():
        return f"http://{AIRFLOW_API_ENDPOINT}/dags"

    @staticmethod
    def __prepare_config_json(metadata: ProjectMetadata):
        use_weather = any(h for h in metadata.get_used_hydrus_models()
                          if h in metadata.hydrus_to_weather.keys())
        return {
            "simulation": {
                "project_id": metadata.project_id,
                "project_minio_location": f"minio/{project_dao.get_project_root(metadata.project_id)}",
                "modflow_model": metadata.modflow_metadata.modflow_id,
                "hydrus_models": list(metadata.get_used_hydrus_models()),
                "use_weather_files": use_weather
            }
        }


airflow_service = AirflowSimulationService()
