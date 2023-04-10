from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import Thread
from typing import List

from .hmse_projects import project_service
from .hmse_projects.project_dao import project_dao
from .hmse_projects.project_metadata import ProjectMetadata
from .hmse_projects.typing_help import ProjectID
from .simulation import simulation_configurator
from .simulation.simulation import Simulation
from .simulation.simulation_chapter import SimulationChapter
from .simulation.simulation_enums import SimulationStageName, SimulationStageStatus
from .simulation.simulation_status import ChapterStatus


@dataclass
class SimulationService(ABC):

    def run_simulation(self, project_metadata: ProjectMetadata) -> None:
        simulation = simulation_configurator.configure_simulation(project_metadata)
        self.register_simulation_if_necessary(simulation)

        project_metadata.finished = False
        project_dao.save_or_update_metadata(project_metadata)

        # Run simulation in background
        thread = Thread(target=simulation.run_simulation)
        thread.start()

    @abstractmethod
    def check_simulation_status(self, project_id: ProjectID) -> ChapterStatus:
        """
        Return status of each step in particular simulation.
        @param project_id: ID of the simulated project to check
        @return: Status of hydrus stage, passing stage and modflow stage (in this exact order)
        """
        ...

    @abstractmethod
    def register_simulation_if_necessary(self, simulation: Simulation):
        ...


class SimulationMockService(SimulationService):

    def run_simulation(self, project_metadata: ProjectMetadata):
        pass

    def check_simulation_status(self, project_id: ProjectID) -> List[ChapterStatus]:
        metadata = project_service.get(project_id)
        simulation = simulation_configurator.configure_simulation(metadata)
        status = simulation.get_simulation_status()
        status[0].set_stage_status(SimulationStageStatus.SUCCESS, stage_idx=0)
        status[0].set_stage_status(SimulationStageStatus.RUNNING, stage_idx=1)
        return status

    def register_simulation_if_necessary(self, simulation: Simulation):
        pass


simulation_service = SimulationMockService()
