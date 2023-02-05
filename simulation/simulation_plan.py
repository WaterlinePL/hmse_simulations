from dataclasses import dataclass
from typing import List

from hmse_simulations.simulation.simulation_chapter import SimulationChapter


@dataclass
class SimulationPlan:
    chapters: List[SimulationChapter]

