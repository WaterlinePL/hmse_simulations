from dataclasses import dataclass, field
from threading import Thread
from typing import List, Dict

import numpy as np

from hmse_simulations.simulation.simulation import Simulation, SimulationID
from hmse_simulations.simulation.simulation_enums import SimulationStage
from hmse_simulations.simulation.simulation_status import SimulationStatus


@dataclass
class SimulationService:
    ids_to_simulations: Dict[SimulationID, Simulation] = field(default_factory=dict)

    def new_simulation(self,
                       project_id: str,
                       spin_up: int,
                       models_to_shapes: List[str, np.ndarray],
                       stages: List[SimulationStage]) -> SimulationID:
        sim = Simulation(project_id=project_id,
                         spin_up=spin_up,
                         models_to_shapes=models_to_shapes,
                         simulation_status=SimulationStatus(stages))

        self.ids_to_simulations[sim.simulation_id] = sim

        # Run simulation in background
        thread = Thread(target=sim.run_simulation)
        thread.start()

        return sim.simulation_id

    def get_simulation_stages(self, simulation_id: str):
        return self.ids_to_simulations[simulation_id].get_stages()

    def check_simulation_status(self, simulation_id: SimulationID) -> SimulationStatus:
        """
        Return status of each step in particular simulation.
        @param simulation_id: Id of the simulation to check
        @return: Status of hydrus stage, passing stage and modflow stage (in this exact order)
        """

        return self.ids_to_simulations[simulation_id].get_simulation_status()
