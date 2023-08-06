import logging
import os
from time import sleep
from typing import Callable

import requests
from requests.auth import HTTPBasicAuth

from ...hmse_projects.project_dao import project_dao
from ...hmse_projects.project_metadata import ProjectMetadata
from ..airflow.airflow_name_converter import convert_hmse_task_to_airflow_task_name
from ..simulation import Simulation
from ..simulation_enums import SimulationStageStatus, SimulationStageName
from ..simulation_error import SimulationError

AIRFLOW_API_ENDPOINT = os.environ["AIRFLOW_API_ENDPOINT"]
AIRFLOW_USER = os.environ["AIRFLOW_USER"]
AIRFLOW_PASSWORD = os.environ["AIRFLOW_PASSWORD"]
AIRFLOW_SIMULATION_DAG = os.environ["AIRFLOW_SIMULATION_DAG"]


class AirflowSimulationService:
    def __init__(self):
        self.auth = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD)

    def start_simulation(self, run_id: str, project_metadata: ProjectMetadata):
        self.__init_activate_dags()
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
            raise SimulationError(description="Failed to connect to Airflow scheduler!")

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
    # TODO
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

    def monitor_job(self, dag_run_id: str, stage_name: SimulationStageName):
        task_id = convert_hmse_task_to_airflow_task_name(stage_name)
        AirflowSimulationService.__monitor_airflow_task(dag_run_id, task_id,
                                                        status_getter_function=self.get_simulation_stage_status)

    def monitor_mapped_job(self, dag_run_id: str, stage_name: SimulationStageName):
        task_id = convert_hmse_task_to_airflow_task_name(stage_name)
        AirflowSimulationService.__monitor_airflow_task(dag_run_id, task_id,
                                                        status_getter_function=self.get_mapped_stage_status)

    @staticmethod
    def __monitor_airflow_task(dag_run_id: str, task_id: str, status_getter_function: Callable):
        done = False
        while not done:
            status = status_getter_function(
                run_id=dag_run_id,
                task_id=task_id
            )
            done = Simulation.handle_stage_status(status)
            sleep(2)


airflow_service = AirflowSimulationService()
