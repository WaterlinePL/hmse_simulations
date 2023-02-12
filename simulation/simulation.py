import datetime
from dataclasses import dataclass
from time import sleep
from typing import List, Callable, Tuple, Optional

import pytz

from ..hmse_projects.project_dao import project_dao
from ..hmse_projects.typing_help import ProjectID
from .airflow_simulation_service import airflow_service
from .simulation_chapter import SimulationChapter
from ..hmse_projects.project_metadata import ProjectMetadata
from .simulation_enums import SimulationStageStatus, SimulationStageName
from .simulation_error import SimulationError
from .simulation_status import ChapterStatus

MODFLOW_OUTPUT_JSON = "results.json"


@dataclass
class Simulation:
    project_metadata: ProjectMetadata
    dag_run_id: Optional[str] = None

    def __post_init__(self):
        self.dag_run_id = Simulation.generate_unique_run_id(self.project_metadata.project_id)

    def __init__(self, project_metadata: ProjectMetadata, sim_chapters: List[SimulationChapter]):
        self.project_metadata = project_metadata
        self.chapter_statuses = [ChapterStatus(chapter, project_metadata) for chapter in sim_chapters]

    # OLD
    # def run_simulation(self) -> None:
    #     airflow_service.start_simulation(self.dag_run_id, self.project_metadata)
    #     for stage, stage_monitor_method in self.__get_stage_methods_to_monitor():
    #         self.simulation_status.set_stage_status(stage, SimulationStageStatus.RUNNING)

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
