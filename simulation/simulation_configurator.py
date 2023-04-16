from typing import List

from .simulation import Simulation
from .simulation_chapter import SimulationChapter
from ..hmse_projects.hmse_hydrological_models.modflow.modflow_step import ModflowStepType
from ..hmse_projects.project_metadata import ProjectMetadata
from ..hmse_projects.simulation_mode import SimulationMode


def configure_simulation(project_metadata: ProjectMetadata) -> Simulation:
    sim_chapters = __chapters_from_metadata(project_metadata)
    return Simulation(project_metadata, sim_chapters)


def __chapters_from_metadata(project_metadata: ProjectMetadata) -> List[SimulationChapter]:
    if project_metadata.simulation_mode == SimulationMode.SIMPLE_COUPLING:
        chapters = [SimulationChapter.SIMPLE_COUPLING]
    elif project_metadata.simulation_mode == SimulationMode.WITH_FEEDBACK:
        modflow_steps = project_metadata.modflow_metadata.steps_info
        starts_steady = modflow_steps[0].type == ModflowStepType.STEADY_STATE
        chapters = [SimulationChapter.FEEDBACK_WARMUP_STEADY_STATE
                    if starts_steady else SimulationChapter.FEEDBACK_WARMUP_TRANSIENT]
        chapters += [SimulationChapter.FEEDBACK_ITERATION for _ in modflow_steps[1:-1]]
        chapters.append(SimulationChapter.SIMULATION_FINAL_ITERATION)
    else:
        raise KeyError("Unknown simulation mode!")

    return chapters
