from enum import auto

from strenum import StrEnum


class SimulationStageStatus(StrEnum):
    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    ERROR = auto()


class SimulationStage(StrEnum):
    INITIALIZATION = auto()
    WEATHER_DATA_TRANSFER = auto()
    HYDRUS_SIMULATION = auto()
    DATA_PASSING = auto()
    MODFLOW_SIMULATION = auto()
    OUTPUT_EXTRACTION_TO_JSON = auto()
    CLEANUP = auto()
