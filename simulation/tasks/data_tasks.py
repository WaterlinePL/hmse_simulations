import os
from abc import ABC, abstractmethod

import flopy
import numpy as np
import phydrus as ph

from .hmse_task import hmse_task
from ..simulation_enums import SimulationStageName
from ..simulation_error import SimulationError
from ...hmse_projects.hmse_hydrological_models.hydrus import hydrus_utils
from ...hmse_projects.hmse_hydrological_models.local_fs_configuration import local_paths
from ...hmse_projects.hmse_hydrological_models.modflow import modflow_utils
from ...hmse_projects.hmse_hydrological_models.weather_data import weather_util
from ...hmse_projects.project_dao import ProjectDao, project_dao
from ...hmse_projects.project_metadata import ProjectMetadata


class DataTasks(ABC):

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.WEATHER_DATA_TRANSFER)
    def weather_data_to_hydrus(project_metadata: ProjectMetadata) -> None:
        project_id = project_metadata.project_id
        sim_dir = local_paths.get_simulation_dir(project_id)
        hydrus_models_to_process = [hydrus_id for hydrus_id in project_metadata.get_used_hydrus_models()
                                    if hydrus_id in project_metadata.hydrus_to_weather]
        for hydrus_id in hydrus_models_to_process:
            weather_id = project_metadata.hydrus_to_weather[hydrus_id]
            hydrus_path = os.path.join(sim_dir, 'hydrus', hydrus_id)
            hydrus_length_unit = hydrus_utils.get_hydrus_length_unit(hydrus_path)
            raw_data = weather_util.read_weather_csv(ProjectDao.get_weather_model_path(project_id, weather_id))
            ready_data = weather_util.adapt_data(raw_data, hydrus_length_unit)
            success = weather_util.add_weather_to_hydrus_model(hydrus_path, ready_data)
            if not success:
                raise SimulationError(f"Error occurred during applying "
                                      f"weather file {weather_id} to hydrus model {hydrus_id}")

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.HYDRUS_TO_MODFLOW_DATA_PASSING)
    def hydrus_to_modflow(project_metadata: ProjectMetadata) -> None:
        project_id = project_metadata.project_id
        modflow_id = project_metadata.modflow_metadata.modflow_id
        sim_dir = local_paths.get_simulation_dir(project_id)
        modflow_path = os.path.join(sim_dir, 'modflow', modflow_id)
        nam_file = modflow_utils.scan_for_modflow_file(modflow_path)

        for mapping_val in project_metadata.get_used_shape_mappings():
            shapes_for_model = [project_dao.get_shape(project_id, shape_id)
                                for shape_id in project_metadata.shapes_to_hydrus.keys()
                                if project_metadata.shapes_to_hydrus[shape_id] == mapping_val]

            # load MODFLOW model - basic info and RCH package
            modflow_model = flopy.modflow.Modflow.load(nam_file, model_ws=modflow_path,
                                                       load_only=["rch"],
                                                       forgive=True)
            spin_up = project_metadata.spin_up

            if isinstance(mapping_val, str):
                hydrus_recharge_path = os.path.join(sim_dir, 'hydrus', mapping_val, 'T_Level.out')
                sum_v_bot = ph.read.read_tlevel(path=hydrus_recharge_path)['sum(vBot)']

                # calc difference for each day (excluding spin_up period)
                sum_v_bot = (-np.diff(sum_v_bot))[spin_up:]
                if spin_up >= len(sum_v_bot):
                    raise SimulationError(description='Spin up is longer than hydrus model time')

            elif isinstance(mapping_val, float):
                sum_v_bot = mapping_val

            else:
                raise SimulationError(description="Unknown mapping in simulation!")

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
                        # average from all hydrus sum(vBot) values during given stress period
                        stress_period_end = stress_period_begin + stress_period_duration
                        if stress_period_begin >= len(sum_v_bot) or stress_period_end >= len(sum_v_bot):
                            raise SimulationError(description=f"Stress period {idx + 1} exceeds hydrus model time")
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

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_TO_HYDRUS_DATA_PASSING)
    def modflow_to_hydrus(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_STEADY_STATE)
    def modflow_init_condition_transfer_steady_state(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.MODFLOW_INIT_CONDITION_TRANSFER_TRANSIENT)
    def modflow_init_condition_transfer_transient(project_metadata: ProjectMetadata) -> None:
        ...
