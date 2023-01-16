from typing import List

from .simulation_enums import SimulationStage, SimulationStageStatus


class SimulationStatus:

    def __init__(self, stages: List[SimulationStage]):
        self.stages = stages
        self.stages_statuses = {stage: SimulationStageStatus.PENDING for stage in stages}

    def get_stages(self) -> List[SimulationStage]:
        return self.stages

    def set_stage_status(self, stage: SimulationStage, new_status: SimulationStageStatus):
        self.stages_statuses[stage] = new_status

    def get_stage_status(self, stage: SimulationStage) -> SimulationStageStatus:
        return self.stages_statuses[stage]

    def to_json(self):
        stage_dict = {step.to_id_and_name()[0]: self.stages_statuses[step] for step in self.stages_statuses.keys()}
        return stage_dict
