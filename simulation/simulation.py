import logging
import time
from typing import List, Optional, ClassVar

from .simulation_chapter import SimulationChapter
from .simulation_enums import SimulationStageStatus, SimulationStageName
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
        self.time_measurements = {}

    def run_simulation(self):
        for chapter in self.chapter_statuses:
            self.__run_chapter(chapter)
        self.project_metadata.finished = True
        project_dao.save_or_update_metadata(self.project_metadata)

    def get_simulation_status(self) -> List[ChapterStatus]:
        return self.chapter_statuses

    def __run_chapter(self, chapter_status: ChapterStatus) -> None:
        chapter_tasks = chapter_status.chapter.get_simulation_tasks(self.project_metadata)
        total_chapter_start = time.time()
        for i, workflow_task in enumerate(chapter_tasks):
            chapter_status.set_stage_status(SimulationStageStatus.RUNNING, stage_idx=i)

            # Launch and monitor stage
            try:
                task_start = time.time()
                workflow_task(self.project_metadata)
                task_end = time.time()
                task_name = str(chapter_status.get_stages_statuses()[i].name)
                self.time_measurements[task_name] = task_end - task_start
            except SimulationError as error:
                chapter_status.set_stage_status(SimulationStageStatus.ERROR, stage_idx=i)
                raise SimulationError(description=error.description)

            chapter_status.set_stage_status(SimulationStageStatus.SUCCESS, stage_idx=i)

            if chapter_status.get_stages_statuses()[i].name == SimulationStageName.MODFLOW_SIMULATION:
                total_chapter_end = time.time()
                self.time_measurements["TOTAL"] = total_chapter_end - total_chapter_start
                logging.info(','.join(self.time_measurements.keys()))
                logging.info(','.join(map(str, self.time_measurements.values())))
