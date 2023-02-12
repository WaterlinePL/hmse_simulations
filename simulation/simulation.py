from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Callable, Tuple, Optional

from .simulation_chapter import SimulationChapter
from ..hmse_projects.project_metadata import ProjectMetadata
from .simulation_enums import SimulationStageStatus, SimulationStageName
from .simulation_error import SimulationError
from .simulation_status import ChapterStatus

MODFLOW_OUTPUT_JSON = "results.json"


class Simulation(ABC):

    def __init__(self, project_metadata: ProjectMetadata, sim_chapters: List[SimulationChapter]):
        self.project_metadata = project_metadata
        self.chapter_statuses = [ChapterStatus(chapter, project_metadata) for chapter in sim_chapters]
        self.simulation_error = None

    ## TODO: steadystate should not be Hydrused??????

    def run_simulation(self):
        for chapter in self.chapter_statuses:
            self.__run_chapter(chapter)

    def get_simulation_status(self) -> List[ChapterStatus]:
        return self.chapter_statuses

    def __run_chapter(self, chapter_status: ChapterStatus) -> None:
        chapter_tasks = chapter_status.chapter.get_simulation_tasks(self.project_metadata)
        for i, workflow_task in enumerate(chapter_tasks):
            chapter_status.set_stage_status(SimulationStageStatus.RUNNING, stage_idx=i)

            # Launch and monitor stage
            try:
                workflow_task(self.project_metadata)
            except SimulationError as error:
                chapter_status.set_stage_status(SimulationStageStatus.ERROR, stage_idx=i)
                raise SimulationError(description=error.description)

            chapter_status.set_stage_status(SimulationStageStatus.SUCCESS, stage_idx=i)
