import datetime
import logging
import time
from dataclasses import dataclass
from typing import List

import pytz

from .airflow.airflow_simulation_service import airflow_service
from .simulation_chapter import SimulationChapter
from .simulation_enums import SimulationStageStatus, SimulationStageName
from .simulation_error import SimulationError
from .simulation_status import ChapterStatus
from ..hmse_projects.project_metadata import ProjectMetadata
from ..hmse_projects.typing_help import ProjectID

MODFLOW_OUTPUT_JSON = "results.json"


@dataclass
class Simulation:
    project_metadata: ProjectMetadata

    def __init__(self, project_metadata: ProjectMetadata, sim_chapters: List[SimulationChapter]):
        self.project_metadata = project_metadata
        self.chapter_statuses = [ChapterStatus(chapter, project_metadata) for chapter in sim_chapters]
        self.time_measurements = {}

    def run_simulation(self):
        airflow_service.init_activate_dags()
        for chapter in self.chapter_statuses:
            self.__run_chapter(chapter)

    def get_simulation_status(self) -> List[ChapterStatus]:
        return self.chapter_statuses

    def __run_chapter(self, chapter_status: ChapterStatus) -> None:
        chapter_tasks = chapter_status.chapter.get_simulation_tasks(self.project_metadata)
        total_chapter_start = time.time()
        dag_run_id = Simulation.generate_unique_run_id(chapter_name=chapter_status.chapter.get_name_snake_case(),
                                                       project_id=self.project_metadata.project_id)
        airflow_service.start_chapter(run_id=dag_run_id,
                                      chapter_name=chapter_status.chapter,
                                      project_metadata=self.project_metadata)

        for i, workflow_task in enumerate(chapter_tasks):
            chapter_status.set_stage_status(SimulationStageStatus.RUNNING, stage_idx=i)

            # Launch and monitor stage
            try:
                task_start = time.time()
                workflow_task(self.project_metadata,
                              dag_run_id=dag_run_id,
                              chapter_name=chapter_status.chapter,
                              stage_name=chapter_status.get_stages_names()[i])
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

    @staticmethod
    def generate_unique_run_id(chapter_name: str, project_id: ProjectID):
        return f"{project_id}-{chapter_name}-{datetime.datetime.now(pytz.timezone('Europe/Warsaw'))}"
