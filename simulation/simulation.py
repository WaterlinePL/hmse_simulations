import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Callable, Tuple, Optional

import numpy as np

from hmse_simulations.simulation.simulation_enums import SimulationStageStatus, SimulationStage
from hmse_simulations.simulation.simulation_error import SimulationError
from hmse_simulations.simulation.simulation_status import SimulationStatus

MODFLOW_OUTPUT_JSON = "results.json"

SimulationID = str  # UUID as string


@dataclass
class Simulation(ABC):
    project_id: str
    spin_up: int
    models_to_shapes: List[str, np.ndarray]
    simulation_status: SimulationStatus
    simulation_error: Optional[SimulationError] = None
    simulation_id: SimulationID = field(default_factory=lambda: str(uuid.uuid4()))

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
    def basic_stages() -> List[SimulationStage]:
        return [
            SimulationStage.INITIALIZATION,
            SimulationStage.HYDRUS_SIMULATION,
            SimulationStage.DATA_PASSING,
            SimulationStage.MODFLOW_SIMULATION,
            SimulationStage.OUTPUT_EXTRACTION_TO_JSON,
            SimulationStage.CLEANUP
        ]

    @staticmethod
    def stages_with_weather_data_transfer() -> List[SimulationStage]:
        stages = Simulation.basic_stages()
        stages.insert(1, SimulationStage.WEATHER_DATA_TRANSFER)
        return stages

    def __get_stage_methods_to_monitor(self) -> List[Tuple[SimulationStage, Callable]]:
        return [(stage, getattr(self, f"__launch_and_monitor_{stage.lower()}"))
                for stage in self.simulation_status.get_stages()]

    @abstractmethod
    def __launch_and_monitor_initialization(self) -> None:
        ...

    @abstractmethod
    def __launch_and_monitor_weather_data_transfer(self) -> None:
        ...

    @abstractmethod
    def __launch_and_monitor_hydrus_simulation(self) -> None:
        ...

    @abstractmethod
    def __launch_and_monitor_data_passing(self) -> None:
        ...

    @abstractmethod
    def __launch_and_monitor_modflow_simulation(self) -> None:
        ...

    @abstractmethod
    def __launch_and_monitor_output_extraction_to_json(self) -> None:
        ...

    @abstractmethod
    def __launch_and_monitor_cleanup(self) -> None:
        ...