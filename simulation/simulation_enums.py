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
    HYDRUS_TO_MODFLOW_DATA_PASSING = auto()
    MODFLOW_SIMULATION = auto()
    OUTPUT_EXTRACTION_TO_JSON = auto()
    CLEANUP = auto()

    INITIALIZE_NEW_ITERATION_FILES = auto()
    CREATE_PER_ZONE_HYDRUS_MODELS = auto()

    MODFLOW_TO_HYDRUS_DATA_PASSING = auto()
    MODFLOW_INIT_CONDITION_TRANSFER_STEADY_STATE = auto()
    MODFLOW_INIT_CONDITION_TRANSFER_TRANSIENT = auto()

    ITERATION_PRE_CONFIGURATION = auto()

    def get_as_id(self) -> str:
        return self.lower().replace('_', ' ').title().replace(' ', '')

    def get_name(self) -> str:
        return {
            SimulationStageName.INITIALIZATION: "Simulation initialization",
            SimulationStageName.WEATHER_DATA_TRANSFER: "Applying weather data to Hydrus models",
            SimulationStageName.HYDRUS_SIMULATION: "Hydrus simulations",
            SimulationStageName.HYDRUS_TO_MODFLOW_DATA_PASSING: "Passing data from Hydrus to Modflow",
            SimulationStageName.MODFLOW_SIMULATION: "Modflow simulation",
            SimulationStageName.OUTPUT_EXTRACTION_TO_JSON: "Exporting output to JSON",
            SimulationStageName.CLEANUP: "Cleaning up after simulation",
            SimulationStageName.INITIALIZE_NEW_ITERATION_FILES: "Initializing files for new iteration",
            SimulationStageName.CREATE_PER_ZONE_HYDRUS_MODELS: "Creating per zone Hydrus models",
            SimulationStageName.MODFLOW_TO_HYDRUS_DATA_PASSING: "Passing data from Modflow to Hydrus",
            SimulationStageName.ITERATION_PRE_CONFIGURATION: "Preconfiguring iteration",

            SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_STEADY_STATE: "Modflow warmup (steady state simulation)"
                                                                              " and zones depth transfer to Hydrus",

            SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_TRANSIENT: "Modflow zones depth transfer to Hydrus "
                                                                           "(BAS file)",
        }[self]


@dataclass
class SimulationStage:
    name: SimulationStageName
    status: SimulationStageStatus
