from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import Thread

from .hmse_projects.project_metadata import ProjectMetadata
from .hmse_projects.typing_help import ProjectID
from .simulation.simulation import Simulation
from .simulation.simulation_enums import SimulationStage, SimulationStageStatus
from .simulation.simulation_status import SimulationStatus


@dataclass
class SimulationService(ABC):

    def run_simulation(self, project_metadata: ProjectMetadata) -> None:
        stages = Simulation.basic_stages()

        if project_metadata.shapes_to_hydrus:
            stages.insert(1, SimulationStage.DATA_PASSING)
            stages.insert(1, SimulationStage.HYDRUS_SIMULATION)

            if project_metadata.hydrus_to_weather:
                stages.insert(1, SimulationStage.WEATHER_DATA_TRANSFER)

        simulation = Simulation(project_metadata=project_metadata,
                                simulation_status=SimulationStatus(stages))

        self.register_simulation_if_necessary(simulation)

        # Run simulation in background
        thread = Thread(target=simulation.run_simulation)
        thread.start()

    @abstractmethod
    def check_simulation_status(self, project_id: ProjectID) -> SimulationStatus:
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

    def check_simulation_status(self, project_id: ProjectID) -> SimulationStatus:
        status = SimulationStatus(Simulation.all_stages())
        status.set_stage_status(SimulationStage.INITIALIZATION, SimulationStageStatus.SUCCESS)
        status.set_stage_status(SimulationStage.WEATHER_DATA_TRANSFER, SimulationStageStatus.RUNNING)
        return status

    def register_simulation_if_necessary(self, simulation: Simulation):
        pass


simulation_service = SimulationMockService()
