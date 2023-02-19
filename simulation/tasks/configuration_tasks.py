import json
import os
import shutil
from abc import ABC, abstractmethod

import flopy
import numpy as np

from .hmse_task import hmse_task
from ..simulation_enums import SimulationStageName
from ...hmse_projects.hmse_hydrological_models.local_fs_configuration import local_paths
from ...hmse_projects.hmse_hydrological_models.modflow import modflow_utils
from ...hmse_projects.project_dao import project_dao
from ...hmse_projects.project_metadata import ProjectMetadata


class ConfigurationTasks(ABC):

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZATION)
    def initialization(project_metadata: ProjectMetadata) -> None:
        project_id = project_metadata.project_id
        sim_dir = local_paths.get_simulation_dir(project_id)

        project_metadata.finished = False
        project_dao.save_or_update_metadata(project_metadata)

        shutil.rmtree(sim_dir, ignore_errors=True)
        os.makedirs(sim_dir)
        shutil.copytree(local_paths.get_hydrus_dir(project_id),
                        local_paths.get_hydrus_dir(project_id, simulation_mode=True))
        shutil.copytree(local_paths.get_modflow_dir(project_id),
                        local_paths.get_modflow_dir(project_id, simulation_mode=True))

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.OUTPUT_EXTRACTION_TO_JSON)
    def output_extraction_to_json(project_metadata: ProjectMetadata) -> None:
        project_id = project_metadata.project_id
        modflow_id = project_metadata.modflow_metadata.modflow_id

        modflow_dir = local_paths.get_modflow_model_path(project_id, modflow_id, simulation_mode=True)
        nam_file = modflow_utils.scan_for_modflow_file(modflow_dir)
        modflow_model = flopy.modflow.Modflow.load(nam_file, model_ws=modflow_dir, forgive=True)
        fhd_path = os.path.join(modflow_dir, modflow_utils.scan_for_modflow_file(modflow_dir, ext=".fhd"))
        modflow_output = flopy.utils.formattedfile.FormattedHeadFile(fhd_path, precision="single")

        result_fhd = np.array([modflow_output.get_data(idx=stress_period)
                               for stress_period in range(modflow_model.nper)])

        with open(local_paths.get_output_json_path(project_id), 'w') as handle:
            json.dump(result_fhd.tolist(), handle, indent=2)

    @staticmethod
    @hmse_task(stage_name=SimulationStageName.CLEANUP)
    def cleanup(project_metadata: ProjectMetadata) -> None:
        # Empty - we do not want to delete results that should be downloaded
        pass

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.INITIALIZE_NEW_ITERATION_FILES)
    def initialize_new_iteration_files(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.CREATE_PER_ZONE_HYDRUS_MODELS)
    def create_per_zone_hydrus_models(project_metadata: ProjectMetadata) -> None:
        ...

    @staticmethod
    @abstractmethod
    @hmse_task(stage_name=SimulationStageName.ITERATION_PRE_CONFIGURATION)
    def iteration_pre_configuration(project_metadata: ProjectMetadata) -> None:
        ...
