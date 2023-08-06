from ..simulation_chapter import SimulationChapter
from ..simulation_enums import SimulationStageName


def convert_chapter_to_dag_name(chapter_name: SimulationChapter) -> str:
    return {
        SimulationChapter.SIMPLE_COUPLING: "hmse_simple_coupling",
        SimulationChapter.FEEDBACK_WARMUP_STEADY_STATE: "hmse_feedback_warmup_steady_state",
        SimulationChapter.FEEDBACK_WARMUP_TRANSIENT: "hmse_feedback_warmup_steady_state",
        SimulationChapter.FEEDBACK_ITERATION: "hmse_feedback_iteration",
        SimulationChapter.FEEDBACK_SIMULATION_FINALIZATION: "hmse_feedback_finalization"
    }[chapter_name]


def convert_hmse_task_to_airflow_task_name(hmse_task: SimulationStageName) -> str:
    return {
        SimulationStageName.INITIALIZATION: "prepare-simulation-volume-content",    # FIXME: possibly needs to be mapped
        SimulationStageName.WEATHER_DATA_TRANSFER: "transfer-weather-files",
        SimulationStageName.HYDRUS_SIMULATION: "hydrus-simulation",
        SimulationStageName.HYDRUS_SIMULATION_WARMUP: "hydrus-simulation",  # TODO: check
        SimulationStageName.HYDRUS_TO_MODFLOW_DATA_PASSING: "transfer-hydrus-results-to-modflow",
        SimulationStageName.MODFLOW_SIMULATION: "modflow-simulation",
        SimulationStageName.OUTPUT_UPLOAD: "upload-simulation-results",
        SimulationStageName.CLEANUP: "cleanup-simulation-volume-content",
        SimulationStageName.INITIALIZE_NEW_ITERATION_FILES: "initialize-feedback-iteration",
        SimulationStageName.SAVE_REFERENCE_HYDRUS_MODELS: "create-reference-hydrus-models",
        SimulationStageName.CREATE_PER_ZONE_HYDRUS_MODELS: "create-per-zone-hydrus-models",
        SimulationStageName.MODFLOW_TO_HYDRUS_DATA_PASSING: "transfer-modflow-results-to-hydrus",
        SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_STEADY_STATE: "transfer-modflow-results-to-hydrus",  # TODO: check
        SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_TRANSIENT: "transfer-modflow-results-to-hydrus",
        SimulationStageName.ITERATION_PRE_CONFIGURATION: "iteration-pre-configuration",
        SimulationStageName.FEEDBACK_SAVE_OUTPUT_ITERATION: "TODO: does not exists"     # TODO
    }[hmse_task]
