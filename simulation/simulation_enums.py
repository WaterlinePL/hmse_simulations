from dataclasses import dataclass
from enum import auto
from typing import Tuple

from strenum import StrEnum


class SimulationStageStatus(StrEnum):
    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    ERROR = auto()

    def is_finished(self):
        return self == SimulationStageStatus.SUCCESS or self == SimulationStageStatus.ERROR


class SimulationStageName(StrEnum):
    INITIALIZATION = auto()
    WEATHER_DATA_TRANSFER = auto()
    HYDRUS_SIMULATION = auto()
    DATA_PASSING = auto()
    MODFLOW_SIMULATION = auto()
    OUTPUT_EXTRACTION_TO_JSON = auto()
    CLEANUP = auto()

    def get_as_id(self) -> str:
        return self.lower().replace('_', ' ').title().replace(' ', '')

    def get_name(self) -> str:
        return {
            SimulationStageName.INITIALIZATION: "Simulation initialization",
            SimulationStageName.WEATHER_DATA_TRANSFER: "Applying weather data to Hydrus models",
            SimulationStageName.HYDRUS_SIMULATION: "Hydrus simulations",
            SimulationStageName.DATA_PASSING: "Passing data from Hydrus to Modflow",
            SimulationStageName.MODFLOW_SIMULATION: "Modflow simulation",
            SimulationStageName.OUTPUT_EXTRACTION_TO_JSON: "Exporting output to JSON",
            SimulationStageName.CLEANUP: "Cleaning up after simulation",
        }[self]


@dataclass
class SimulationStage:
    name: SimulationStageName
    status: SimulationStageStatus
