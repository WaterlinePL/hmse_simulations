from dataclasses import dataclass, field
from threading import Thread
from typing import Dict, List

from .hmse_projects.project_dao import project_dao
from .hmse_projects.project_metadata import ProjectMetadata
from .hmse_projects.typing_help import ProjectID
from .simulation import simulation_configurator
from .simulation.simulation import Simulation
from .simulation.simulation_status import ChapterStatus


@dataclass
class SimulationService:
    project_ids_to_run_ids: Dict[ProjectID, Simulation] = field(default_factory=dict)

    def run_simulation(self, project_metadata: ProjectMetadata) -> None:
        simulation = simulation_configurator.configure_simulation(project_metadata)
        self.register_simulation_if_necessary(simulation)

        project_metadata.finished = False
        project_dao.save_or_update_metadata(project_metadata)

        # Run simulation in background
        thread = Thread(target=simulation.run_simulation)
        thread.start()

    def check_simulation_status(self, project_id: ProjectID) -> List[ChapterStatus]:
        """
        Return status of each chapter in particular simulation.
        @param project_id: ID of the simulated project to check
        @return: Status of hydrus stage, passing stage and modflow stage (in this exact order)
        """

        all_chapter_statuses = self.project_ids_to_run_ids[project_id].get_simulation_status()
        if all_chapter_statuses[-1].get_stages_statuses()[-1].status.is_finished():
            del self.project_ids_to_run_ids[project_id]
        return all_chapter_statuses

    def register_simulation_if_necessary(self, simulation: Simulation):
        self.project_ids_to_run_ids[simulation.project_metadata.project_id] = simulation


simulation_service = SimulationService()
