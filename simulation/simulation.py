import json
import os
import shutil
import subprocess
from abc import ABC
from dataclasses import dataclass
from typing import List, Callable, Tuple, Optional

import flopy
import numpy as np
import phydrus as ph

from config import app_config
from hmse_simulations import path_formatter
from hmse_simulations.hmse_projects.hmse_hydrological_models.hydrus import hydrus_utils
from hmse_simulations.hmse_projects.hmse_hydrological_models.modflow import modflow_utils
from hmse_simulations.hmse_projects.hmse_hydrological_models.weather_data import weather_util
from hmse_simulations.hmse_projects.project_dao import WORKSPACE_PATH, ProjectDao, project_dao
from hmse_simulations.hmse_projects.project_metadata import ProjectMetadata
from hmse_simulations.hmse_projects.typing_help import ProjectID
from hmse_simulations.simulation.simulation_enums import SimulationStageStatus, SimulationStage
from hmse_simulations.simulation.simulation_error import SimulationError
from hmse_simulations.simulation.simulation_status import SimulationStatus

MODFLOW_OUTPUT_JSON = "results.json"
SIMULATION_DIR = "simulation"


@dataclass
class Simulation(ABC):
    project_metadata: ProjectMetadata
    simulation_status: SimulationStatus
    simulation_error: Optional[SimulationError] = None

    def get_simulation_status(self):
        return self.simulation_status

    def run_simulation(self) -> None:
        for stage, stage_monitor_method in self.__get_stage_methods_to_monitor():
            self.simulation_status.set_stage_status(stage, SimulationStageStatus.RUNNING)

            # Launch and monitor stage
            try:
                stage_monitor_method()
            except SimulationError as error:
                self.simulation_error = error
                self.simulation_status.set_stage_status(stage, SimulationStageStatus.ERROR)
                return

            self.simulation_status.set_stage_status(stage, SimulationStageStatus.SUCCESS)

    def get_stages(self):
        return self.simulation_status.get_stages()

    @staticmethod
    def all_stages() -> List[SimulationStage]:
        """
        This method is used to show the whole flow of simulation.
        @return: All possible stages in simulation
        """
        return [
            SimulationStage.INITIALIZATION,
            SimulationStage.WEATHER_DATA_TRANSFER,
            SimulationStage.HYDRUS_SIMULATION,
            SimulationStage.DATA_PASSING,
            SimulationStage.MODFLOW_SIMULATION,
            SimulationStage.OUTPUT_EXTRACTION_TO_JSON,
            SimulationStage.CLEANUP
        ]

    @staticmethod
    def basic_stages() -> List[SimulationStage]:
        return [
            SimulationStage.INITIALIZATION,
            SimulationStage.MODFLOW_SIMULATION,
            SimulationStage.OUTPUT_EXTRACTION_TO_JSON,
            SimulationStage.CLEANUP
        ]

    def __get_stage_methods_to_monitor(self) -> List[Tuple[SimulationStage, Callable]]:
        return [(stage, getattr(self, f"launch_and_monitor_{stage.lower()}"))
                for stage in self.simulation_status.get_stages()]

    def launch_and_monitor_initialization(self) -> None:
        project_id = self.project_metadata.project_id
        sim_dir = Simulation.__get_simulation_dir(project_id)

        self.project_metadata.finished = False
        project_dao.save_or_update_metadata(self.project_metadata)

        shutil.rmtree(sim_dir, ignore_errors=True)
        os.makedirs(sim_dir)
        shutil.copytree(os.path.join(WORKSPACE_PATH, project_id, 'hydrus'), os.path.join(sim_dir, 'hydrus'))
        shutil.copytree(os.path.join(WORKSPACE_PATH, project_id, 'modflow'), os.path.join(sim_dir, 'modflow'))

    def launch_and_monitor_weather_data_transfer(self) -> None:
        project_id = self.project_metadata.project_id
        sim_dir = Simulation.__get_simulation_dir(project_id)
        hydrus_models_to_process = [hydrus_id for hydrus_id in self.__get_used_hydrus_models()
                                    if hydrus_id in self.project_metadata.hydrus_to_weather]
        for hydrus_id in hydrus_models_to_process:
            weather_id = self.project_metadata.hydrus_to_weather[hydrus_id]
            hydrus_path = os.path.join(sim_dir, 'hydrus', hydrus_id)
            hydrus_length_unit = hydrus_utils.get_hydrus_length_unit(hydrus_path)
            raw_data = weather_util.read_weather_csv(ProjectDao.get_weather_model_path(project_id, weather_id))
            ready_data = weather_util.adapt_data(raw_data, hydrus_length_unit)
            success = weather_util.add_weather_to_hydrus_model(hydrus_path, ready_data)
            if not success:
                raise SimulationError(f"Error occurred during applying "
                                      f"weather file {weather_id} to hydrus model {hydrus_id}")

    def launch_and_monitor_hydrus_simulation(self) -> None:
        simulations = []
        for hydrus_id in self.__get_used_hydrus_models():
            path = os.path.join(Simulation.__get_simulation_dir(self.project_metadata.project_id), 'hydrus', hydrus_id)
            hydrus_exec_path = path_formatter.convert_backslashes_to_slashes(
                app_config.get_config().hydrus_program_path)
            proc = Simulation.__run_local_program(exec_path=hydrus_exec_path,
                                                  args=[path_formatter.convert_backslashes_to_slashes(path)])
            simulations.append(proc)

        for proc in simulations:
            proc.communicate(input="\n")  # Press enter to close program (blocking)

    def launch_and_monitor_data_passing(self) -> None:
        project_id = self.project_metadata.project_id
        modflow_id = self.project_metadata.modflow_metadata.modflow_id
        sim_dir = Simulation.__get_simulation_dir(project_id)
        modflow_path = os.path.join(sim_dir, 'modflow', modflow_id)
        nam_file = modflow_utils.scan_for_modflow_file(modflow_path)

        for mapping_val in self.__get_used_shape_mappings():
            shapes_for_model = [project_dao.get_shape(project_id, shape_id)
                                for shape_id in self.project_metadata.shapes_to_hydrus.keys()
                                if self.project_metadata.shapes_to_hydrus[shape_id] == mapping_val]

            # load MODFLOW model - basic info and RCH package
            modflow_model = flopy.modflow.Modflow.load(nam_file, model_ws=modflow_path,
                                                       load_only=["rch"],
                                                       forgive=True)
            spin_up = self.project_metadata.spin_up

            if isinstance(mapping_val, str):
                hydrus_recharge_path = os.path.join(sim_dir, 'hydrus', mapping_val, 'T_Level.out')
                sum_v_bot = ph.read.read_tlevel(path=hydrus_recharge_path)['sum(vBot)']

                if spin_up >= len(sum_v_bot):
                    raise ValueError('Spin up is longer than hydrus model time')  # TODO

            elif isinstance(mapping_val, float):
                sum_v_bot = mapping_val

            else:
                raise Exception("Unknown mapping in simulation!")  # TODO

            for shape in shapes_for_model:
                mask = (shape == 1)  # Frontend sets explicitly 1

                stress_period_begin = 0  # beginning of current stress period
                for idx, stress_period_duration in enumerate(modflow_model.modeltime.perlen):
                    # float -> int indexing purposes
                    stress_period_duration = int(stress_period_duration)

                    # modflow rch array for given stress period
                    recharge_modflow_array = modflow_model.rch.rech[idx].array

                    if isinstance(sum_v_bot, float):
                        avg_v_bot_stress_period = sum_v_bot
                    else:
                        # calc difference for each day (excluding spin_up period)
                        sum_v_bot = (-np.diff(sum_v_bot))[spin_up:]

                        # average from all hydrus sum(vBot) values during given stress period
                        stress_period_end = stress_period_begin + stress_period_duration
                        if stress_period_begin >= len(sum_v_bot) or stress_period_end >= len(sum_v_bot):
                            raise ValueError("Stress period " + str(idx + 1) + " is out of hydrus model time")
                        avg_v_bot_stress_period = np.average(sum_v_bot[stress_period_begin:stress_period_end])

                    # add calculated hydrus average sum(vBot) to modflow recharge array
                    recharge_modflow_array[mask] = avg_v_bot_stress_period

                    # save calculated recharge to modflow model
                    modflow_model.rch.rech[idx] = recharge_modflow_array

                    # update beginning of current stress period
                    stress_period_begin += stress_period_duration

            new_recharge = modflow_model.rch.rech
            rch_package = modflow_model.get_package("rch")  # get the RCH package

            # generate and save new RCH (same properties, different recharge)
            flopy.modflow.ModflowRch(modflow_model, nrchop=rch_package.nrchop, ipakcb=rch_package.ipakcb,
                                     rech=new_recharge,
                                     irch=rch_package.irch).write_file(check=False)

    def launch_and_monitor_modflow_simulation(self) -> None:
        sim_dir = Simulation.__get_simulation_dir(self.project_metadata.project_id)
        modflow_id = self.project_metadata.modflow_metadata.modflow_id
        modflow_path = os.path.join(sim_dir, 'modflow', modflow_id)

        current_dir = os.getcwd()

        modflow_exec_path = path_formatter.convert_backslashes_to_slashes(app_config.get_config().modflow_program_path)
        nam_file = modflow_utils.scan_for_modflow_file(modflow_path)

        os.chdir(modflow_path)
        proc = Simulation.__run_local_program(exec_path=modflow_exec_path,
                                              args=[nam_file])
        os.chdir(current_dir)
        proc.communicate(input="\n")  # Press enter to close program (blocking)

    def launch_and_monitor_output_extraction_to_json(self) -> None:
        project_id = self.project_metadata.project_id
        modflow_id = self.project_metadata.modflow_metadata.modflow_id
        sim_dir = Simulation.__get_simulation_dir(project_id)

        modflow_dir = os.path.join(sim_dir, 'modflow', modflow_id)
        nam_file = modflow_utils.scan_for_modflow_file(modflow_dir)
        modflow_model = flopy.modflow.Modflow.load(nam_file, model_ws=modflow_dir, forgive=True)
        fhd_path = os.path.join(modflow_dir, modflow_utils.scan_for_modflow_file(modflow_dir, ext=".fhd"))
        modflow_output = flopy.utils.formattedfile.FormattedHeadFile(fhd_path, precision="single")

        result_fhd = np.array([modflow_output.get_data(idx=stress_period)
                               for stress_period in range(modflow_model.nper)])

        result_path = os.path.join(sim_dir, MODFLOW_OUTPUT_JSON)
        with open(result_path, 'w') as handle:
            json.dump(result_fhd.tolist(), handle, indent=2)

    def launch_and_monitor_cleanup(self) -> None:
        # Empty - we do not want to delete results that should be downloaded
        self.project_metadata.finished = True
        project_dao.save_or_update_metadata(self.project_metadata)

    @staticmethod
    def __get_simulation_dir(project_id: ProjectID):
        return os.path.join(WORKSPACE_PATH, project_id, SIMULATION_DIR)

    @staticmethod
    def __run_local_program(exec_path: str, args: List[str], log_handle=None):
        return subprocess.Popen([exec_path, *args],
                                shell=True, text=True,
                                stdin=subprocess.PIPE, stdout=log_handle, stderr=log_handle)

    def __get_used_hydrus_models(self):
        return {hydrus_id for hydrus_id in self.project_metadata.shapes_to_hydrus.values()
                if isinstance(hydrus_id, str)}

    def __get_used_shape_mappings(self):
        return {mapping_value for mapping_value in self.project_metadata.shapes_to_hydrus.values()}
