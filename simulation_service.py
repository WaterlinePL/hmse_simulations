from dataclasses import dataclass
from threading import Thread

from .hmse_projects.project_metadata import ProjectMetadata
from .hmse_projects.typing_help import ProjectID
from .simulation.simulation import Simulation
from .simulation.simulation_enums import SimulationStage
from .simulation.simulation_status import SimulationStatus


@dataclass
class SimulationService:

    def __init__(self):
        self.project_ids_to_run_ids = {}

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

    def check_simulation_status(self, project_id: ProjectID) -> SimulationStatus:
        """
        Return status of each step in particular simulation.
        @param project_id: ID of the simulated project to check
        @return: Status of hydrus stage, passing stage and modflow stage (in this exact order)
        """

        status = self.project_ids_to_run_ids[project_id].get_simulation_status()
        if status.get_stage_status(SimulationStage.CLEANUP).is_finished():
            del self.project_ids_to_run_ids[project_id]
        return status

    def register_simulation_if_necessary(self, simulation: Simulation):
        self.project_ids_to_run_ids[simulation.project_metadata.project_id] = simulation


simulation_service = SimulationService()
