import logging
import os
from time import sleep
from typing import Callable, Dict

import requests
from requests.auth import HTTPBasicAuth

from ..airflow.airflow_name_converter import convert_hmse_task_to_airflow_task_name, convert_chapter_to_dag_name
from ..simulation_enums import SimulationStageStatus, SimulationStageName
from ..simulation_error import SimulationError
from ...hmse_projects.minio_controller.minio_controller import minio_controller
from ...hmse_projects.project_dao import project_dao
from ...hmse_projects.project_metadata import ProjectMetadata
from ...hmse_projects.simulation_mode import SimulationMode

AIRFLOW_API_ENDPOINT = os.environ["AIRFLOW_API_ENDPOINT"]
AIRFLOW_USER = os.environ["AIRFLOW_USER"]
AIRFLOW_PASSWORD = os.environ["AIRFLOW_PASSWORD"]


class AirflowSimulationService:
    def __init__(self):
        self.auth = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD)

    def get_simulation_stage_status(self,
                                    run_id: str,
                                    chapter_name: 'SimulationChapter',
                                    task_id: str) -> SimulationStageStatus:

        resp = requests.get(self.__get_endpoint_for_task(run_id, chapter_name, task_id), auth=self.auth)
        resp.raise_for_status()
        status = resp.json()["state"]
        return AirflowSimulationService.__analyze_single_status(status)

    def get_mapped_stage_status(self,
                                run_id: str,
                                chapter_name: 'SimulationChapter',
                                task_id: str) -> SimulationStageStatus:

        mapped_endpoint = f"{self.__get_endpoint_for_task(run_id, chapter_name, task_id)}/listMapped"
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

    def init_activate_dags(self):
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

    def start_chapter(self, run_id: str, chapter_name: 'SimulationChapter', project_metadata: ProjectMetadata):
        req_content = {
            "conf": AirflowSimulationService.__prepare_config_json_new(project_metadata),
            "dag_run_id": run_id
        }
        resp = requests.post(AirflowSimulationService.__get_endpoint_for_simulation_start(chapter_name),
                             json=req_content,
                             auth=self.auth)
        if resp.status_code != 200:
            raise SimulationError(description="Chapter failed to start!")

    @staticmethod
    def __get_endpoint_for_task(run_id: str, chapter_name: 'SimulationChapter', task_id: str) -> str:
        dag_name = convert_chapter_to_dag_name(chapter_name)
        return (f"http://{AIRFLOW_API_ENDPOINT}/dags/{dag_name}/dagRuns/{run_id}/"
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
    def __get_endpoint_for_simulation_start(chapter_name: 'SimulationChapter'):
        dag_name = convert_chapter_to_dag_name(chapter_name)
        return f"http://{AIRFLOW_API_ENDPOINT}/dags/{dag_name}/dagRuns"

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

    @staticmethod
    def __prepare_config_json_new(metadata: ProjectMetadata) -> Dict:
        s3_location_prefix = minio_controller.get_s3_prefix()
        s3_location = f"{s3_location_prefix}{project_dao.get_project_root(metadata.project_id)}"
        simulation_config = metadata.to_json_response().__dict__
        if "modflow_metadata" in simulation_config:
            simulation_config["modflow_metadata"] = simulation_config["modflow_metadata"].to_json()
        simulation_config["project_minio_location"] = s3_location
        simulation_config["is_feedback_loop"] = metadata.simulation_mode == SimulationMode.WITH_FEEDBACK
        return {"simulation": simulation_config}

    def monitor_job(self, dag_run_id: str, chapter_name: 'SimulationChapter', stage_name: SimulationStageName):
        task_id = convert_hmse_task_to_airflow_task_name(stage_name)
        AirflowSimulationService.__monitor_airflow_task(dag_run_id, chapter_name, task_id,
                                                        status_getter_function=self.get_simulation_stage_status)

    def monitor_mapped_job(self, dag_run_id: str, chapter_name: 'SimulationChapter', stage_name: SimulationStageName):
        task_id = convert_hmse_task_to_airflow_task_name(stage_name)
        AirflowSimulationService.__monitor_airflow_task(dag_run_id, chapter_name, task_id,
                                                        status_getter_function=self.get_mapped_stage_status)

    @staticmethod
    def __monitor_airflow_task(dag_run_id: str,
                               chapter_name: 'SimulationChapter',
                               task_id: str,
                               status_getter_function: Callable):
        done = False
        while not done:
            status = status_getter_function(
                run_id=dag_run_id,
                chapter_name=chapter_name,
                task_id=task_id
            )
            done = AirflowSimulationService.handle_stage_status(status)
            sleep(2)

    @staticmethod
    def handle_stage_status(stage_status: SimulationStageStatus) -> bool:
        if stage_status == SimulationStageStatus.ERROR:
            raise SimulationError(f"Step {stage_status} has failed!")
        if stage_status == SimulationStageStatus.SUCCESS:
            return True
        return False


airflow_service = AirflowSimulationService()
