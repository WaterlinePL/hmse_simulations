from abc import ABC, abstractmethod
from dataclasses import dataclass
from time import sleep
from typing import List, Callable, Tuple, Optional

from hmse_simulations.hmse_projects.project_metadata import ProjectMetadata
from hmse_simulations.simulation.airflow_simulation_service import airflow_service
from hmse_simulations.simulation.simulation_enums import SimulationStageStatus, SimulationStage
from hmse_simulations.simulation.simulation_error import SimulationError
from hmse_simulations.simulation.simulation_status import SimulationStatus

MODFLOW_OUTPUT_JSON = "results.json"


@dataclass
class Simulation:
    project_metadata: ProjectMetadata
    simulation_status: SimulationStatus
    simulation_error: Optional[SimulationError] = None
    dag_run_id: Optional[str] = None

    def set_dag_run_id(self, dag_run_id: str):
        self.dag_run_id = dag_run_id

    def get_simulation_status(self):
        return self.simulation_status

    def run_simulation(self) -> None:
        for stage, stage_monitor_method in self.__get_stage_methods_to_monitor():
            self.simulation_status.set_stage_status(stage, SimulationStageStatus.RUNNING)

            # Launch and monitor stage
            try:
                stage_monitor_method()
            except SimulationError as error:
                self.simulation_status.set_stage_status(stage, SimulationStageStatus.ERROR)
                raise SimulationError(description=error.description)

            self.simulation_status.set_stage_status(stage, SimulationStageStatus.SUCCESS)

    def get_stages(self):
        return self.simulation_status.get_stages()

    @staticmethod
    def all_stages() -> List[SimulationStage]:
        """
        This method is used to show the whole flow of simulation.
        @return: All possible stages in simulation
        """
        return [
            SimulationStage.INITIALIZATION,
            SimulationStage.WEATHER_DATA_TRANSFER,
            SimulationStage.HYDRUS_SIMULATION,
            SimulationStage.DATA_PASSING,
            SimulationStage.MODFLOW_SIMULATION,
            SimulationStage.OUTPUT_UPLOAD,
            SimulationStage.CLEANUP
        ]

    @staticmethod
    def basic_stages() -> List[SimulationStage]:
        return [
            SimulationStage.INITIALIZATION,
            SimulationStage.MODFLOW_SIMULATION,
            SimulationStage.OUTPUT_UPLOAD,
            SimulationStage.CLEANUP
        ]

    def __get_stage_methods_to_monitor(self) -> List[Tuple[SimulationStage, Callable]]:
        return [(stage, getattr(self, f"launch_and_monitor_{stage.lower()}"))
                for stage in self.simulation_status.get_stages()]

    def launch_and_monitor_initialization(self) -> None:
        self.__monitor_job(task_id="prepare-simulation-volume-content")

    def launch_and_monitor_weather_data_transfer(self) -> None:
        self.__monitor_job(task_id="transfer-weather-files")

    def launch_and_monitor_hydrus_simulation(self) -> None:
        self.__monitor_mapped_job(task_id="hydrus-simulation")

    def launch_and_monitor_data_passing(self) -> None:
        self.__monitor_job(task_id="transfer-hydrus-results-to-modflow")

    def launch_and_monitor_modflow_simulation(self) -> None:
        self.__monitor_mapped_job(task_id="modflow-simulation")

    def launch_and_monitor_output_upload(self) -> None:
        self.__monitor_job(task_id="upload-simulation-results")

    def launch_and_monitor_cleanup(self) -> None:
        self.__monitor_job(task_id="cleanup-simulation-volume-content")

    def __monitor_job(self, task_id: str):
        self.__monitor_airflow_task(task_id, status_getter_function=airflow_service.get_simulation_stage_status)

    def __monitor_mapped_job(self, task_id: str):
        self.__monitor_airflow_task(task_id, status_getter_function=airflow_service.get_mapped_stage_status)

    def __monitor_airflow_task(self, task_id: str, status_getter_function: Callable):
        done = False
        while not done:
            status = status_getter_function(
                run_id=self.dag_run_id,
                task_id=task_id
            )
            done = Simulation.__handle_stage_status(status)
            sleep(2)

    @staticmethod
    def __handle_stage_status(stage_status: SimulationStageStatus) -> bool:
        if stage_status == SimulationStageStatus.ERROR:
            raise SimulationError(f"Step {stage_status} has failed!")
        if stage_status == SimulationStageStatus.SUCCESS:
            return True
        return False
