import subprocess
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List

from .hmse_task import hmse_task
from ..deployment.hydrus_docker_deployer import HydrusDockerDeployer
from ..deployment.modflow2005_docker_deployer import ModflowDockerDeployer
from ..simulation import Simulation
from ..simulation_enums import SimulationStageName
from ...hmse_projects.hmse_hydrological_models.hydrus import hydrus_utils
from ...hmse_projects.project_metadata import ProjectMetadata
from ...hmse_projects.simulation_mode import SimulationMode


class SimulationTasks:

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_SIMULATION)
    def hydrus_simulation(project_metadata: ProjectMetadata) -> None:
        simulations = []
        if project_metadata.simulation_mode == SimulationMode.SIMPLE_COUPLING:
            hydrus_to_launch = hydrus_utils.get_used_hydrus_models(project_metadata.shapes_to_hydrus)
        else:
            hydrus_to_launch = hydrus_utils.get_compound_hydrus_ids_for_feedback_loop(project_metadata.shapes_to_hydrus)
            hydrus_to_launch = [compound_hydrus_id for _, compound_hydrus_id in hydrus_to_launch]

        with ThreadPoolExecutor(max_workers=Simulation.MAX_CONCURRENT_MODELS) as exe:
            for hydrus_id in hydrus_to_launch:
                sim = HydrusDockerDeployer(project_metadata.project_id, hydrus_id)
                sim.run_simulation_image()
                simulations.append(exe.submit(sim.wait_for_termination))
            wait(simulations)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_SIMULATION)
    def modflow_simulation(project_metadata: ProjectMetadata) -> None:
        deployer = ModflowDockerDeployer(project_metadata.project_id,
                                         project_metadata.modflow_metadata.modflow_id)
        deployer.run_simulation_image()
        deployer.wait_for_termination()


    @staticmethod
    def __run_local_program(exec_path: str, args: List[str], log_handle=None):
        return subprocess.Popen([exec_path, *args],
                                shell=True, text=True,
                                stdin=subprocess.PIPE, stdout=log_handle, stderr=log_handle)
