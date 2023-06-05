from typing import List, Optional, ClassVar

from .simulation_chapter import SimulationChapter
from .simulation_enums import SimulationStageStatus
from .simulation_error import SimulationError
from .simulation_status import ChapterStatus
from ..hmse_projects.project_dao import project_dao
from ..hmse_projects.project_metadata import ProjectMetadata


class Simulation:

    project_metadata: ProjectMetadata
    simulation_status: List[ChapterStatus]
    simulation_error: Optional[SimulationError] = None

    def __init__(self, project_metadata: ProjectMetadata, sim_chapters: List[SimulationChapter]):
        self.project_metadata = project_metadata
        self.chapter_statuses = [ChapterStatus(chapter, project_metadata) for chapter in sim_chapters]
        self.simulation_error = None

    def run_simulation(self):
        for chapter in self.chapter_statuses:
            self.__run_chapter(chapter)
        self.project_metadata.finished = True
        project_dao.save_or_update_metadata(self.project_metadata)

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
