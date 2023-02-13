import os
import subprocess
from abc import ABC, abstractmethod
from typing import List

from config import app_config
from .hmse_task import hmse_task
from ..simulation_enums import SimulationStageName
from ... import path_formatter
from ...hmse_projects.hmse_hydrological_models.local_fs_configuration import local_paths
from ...hmse_projects.hmse_hydrological_models.modflow import modflow_utils
from ...hmse_projects.project_metadata import ProjectMetadata


class SimulationTasks(ABC):

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_SIMULATION)
    def hydrus_simulation(project_metadata: ProjectMetadata) -> None:
        simulations = []
        for hydrus_id in project_metadata.get_used_hydrus_models():
            path = os.path.join(local_paths.get_simulation_dir(project_metadata.project_id), 'hydrus', hydrus_id)
            # FIXME: bad design - upward reference
            hydrus_exec_path = path_formatter.convert_backslashes_to_slashes(
                app_config.get_config().hydrus_program_path)
            proc = SimulationTasks.__run_local_program(exec_path=hydrus_exec_path,
                                                       args=[path_formatter.convert_backslashes_to_slashes(path)])
            simulations.append(proc)

        for proc in simulations:
            proc.communicate(input="\n")  # Press enter to close program (blocking)

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_SIMULATION)
    def modflow_simulation(project_metadata: ProjectMetadata) -> None:
        sim_dir = local_paths.get_simulation_dir(project_metadata.project_id)
        modflow_id = project_metadata.modflow_metadata.modflow_id
        modflow_path = os.path.join(sim_dir, 'modflow', modflow_id)

        current_dir = os.getcwd()

        # FIXME: bad design - upward reference
        modflow_exec_path = path_formatter.convert_backslashes_to_slashes(app_config.get_config().modflow_program_path)
        nam_file = modflow_utils.scan_for_modflow_file(modflow_path)

        os.chdir(modflow_path)
        proc = SimulationTasks.__run_local_program(exec_path=modflow_exec_path,
                                                   args=[nam_file])
        os.chdir(current_dir)
        proc.communicate(input="\n")  # Press enter to close program (blocking)

    @staticmethod
    def __run_local_program(exec_path: str, args: List[str], log_handle=None):
        return subprocess.Popen([exec_path, *args],
                                shell=True, text=True,
                                stdin=subprocess.PIPE, stdout=log_handle, stderr=log_handle)
