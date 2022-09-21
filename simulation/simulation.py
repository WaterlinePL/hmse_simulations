from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Callable, Tuple, Optional

from hmse_simulations.hmse_projects.project_metadata import ProjectMetadata
from hmse_simulations.simulation.simulation_enums import SimulationStageStatus, SimulationStage
from hmse_simulations.simulation.simulation_error import SimulationError
from hmse_simulations.simulation.simulation_status import SimulationStatus

MODFLOW_OUTPUT_JSON = "results.json"


@dataclass
class Simulation(ABC):
    project_metadata: ProjectMetadata
    simulation_status: SimulationStatus
    simulation_error: Optional[SimulationError] = None

    def get_simulation_status(self):
        return self.simulation_status

    def run_simulation(self) -> None:
        for stage, stage_monitor_method in self.__get_stage_methods_to_monitor():
            self.simulation_status.set_stage_status(stage, SimulationStageStatus.RUNNING)

            # Launch and monitor stage
            try:
                stage_monitor_method()
            except SimulationError as error:
                self.simulation_error = error
                self.simulation_status.set_stage_status(stage, SimulationStageStatus.ERROR)
                return

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
            SimulationStage.OUTPUT_EXTRACTION_TO_JSON,
            SimulationStage.CLEANUP
        ]

    @staticmethod
    def basic_stages() -> List[SimulationStage]:
        return [
            SimulationStage.INITIALIZATION,
            SimulationStage.MODFLOW_SIMULATION,
            SimulationStage.OUTPUT_EXTRACTION_TO_JSON,
            SimulationStage.CLEANUP
        ]

    def __get_stage_methods_to_monitor(self) -> List[Tuple[SimulationStage, Callable]]:
        return [(stage, getattr(self, f"launch_and_monitor_{stage.lower()}"))
                for stage in self.simulation_status.get_stages()]

    @abstractmethod
    def launch_and_monitor_initialization(self) -> None:
        ...

    @abstractmethod
    def launch_and_monitor_weather_data_transfer(self) -> None:
        ...

    @abstractmethod
    def launch_and_monitor_hydrus_simulation(self) -> None:
        ...

    @abstractmethod
    def launch_and_monitor_data_passing(self) -> None:
        ...

    @abstractmethod
    def launch_and_monitor_modflow_simulation(self) -> None:
        ...

    @abstractmethod
    def launch_and_monitor_output_extraction_to_json(self) -> None:
        ...

    @abstractmethod
    def launch_and_monitor_cleanup(self) -> None:
        ...
