from dataclasses import dataclass
from typing import List

from hmse_simulations.simulation.simulation_enums import SimulationStage


@dataclass
class SimulationChapter:
    stages: List[SimulationStage]