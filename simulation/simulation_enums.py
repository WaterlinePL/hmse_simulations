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


class SimulationStage(StrEnum):
    INITIALIZATION = auto()
    WEATHER_DATA_TRANSFER = auto()
    HYDRUS_SIMULATION = auto()
    DATA_PASSING = auto()
    MODFLOW_SIMULATION = auto()
    OUTPUT_UPLOAD = auto()
    CLEANUP = auto()

    def __get_name(self) -> str:
        return {
            SimulationStage.INITIALIZATION: "Simulation initialization",
            SimulationStage.WEATHER_DATA_TRANSFER: "Applying weather data to Hydrus models",
            SimulationStage.HYDRUS_SIMULATION: "Hydrus simulations",
            SimulationStage.DATA_PASSING: "Passing data from Hydrus to Modflow",
            SimulationStage.MODFLOW_SIMULATION: "Modflow simulation",
            SimulationStage.OUTPUT_UPLOAD: "Uploading output to remote drive",
            SimulationStage.CLEANUP: "Cleaning up after simulation",
        }[self]

    def to_id_and_name(self) -> Tuple[str, str]:
        my_id = self.lower().replace('_', ' ').title().replace(' ', '')
        return my_id, self.__get_name()
