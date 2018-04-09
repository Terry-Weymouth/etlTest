import os
import logging
from materials_commons.etl.input_spreadsheet import BuildProjectExperiment
from .DB import DbConnection
from .globus_service import MaterialsCommonsGlobusInterface
from .VerifySetup import VerifySetup
import configparser


GLOBUS_QUEUE = 'elt:globus-transfer'
PROCESS_QUEUE = 'etl:build-experiment'

class ETLWorker:
    def __init__(self, user_id):
        self.user_id = user_id
        self.log = logging.getLogger(__name__ + "." + self.__class__.__name__)
        user_endoint_config_file_path = os.path.join('.globus_test', 'endpoint.ini')
        config_file_locaton_for_user_endpoint = os.path.join(os.path.expanduser("~"), user_endoint_config_file_path)
        config = configparser.ConfigParser()
        config.read(str(config_file_locaton_for_user_endpoint))
        self.worker_base_path = config['worker']['base_path']

    def run_with(self, project_id, experiment_name, experiment_description,
                 globus_endpoint, endpoint_path, request_uuid,
                 excel_file_relative_path, data_dir_relitive_path):

        base_path = self.worker_base_path
        transfer_base_path = "{}/transfer-{}".format(base_path, request_uuid)
        excel_file_path = "{}/{}".format(transfer_base_path, excel_file_relative_path)
        data_file_path = "{}/{}".format(transfer_base_path, data_dir_relitive_path)
        self.log.info("excel_file_path = " + excel_file_path)
        self.log.info("data_file_path = " + data_file_path)
        status_record = self.create_status_record(project_id, VERIFYING_SETUP)
        results = self.verify_preconditions(
            project_id, globus_endpoint, endpoint_path, transfer_base_path,
            excel_file_relative_path, data_dir_relitive_path
        )
        if not results['status'] == 'SUCCEEDED':
            self.log.error("Preconditions for transfer failed...")
            for key in results:
                self.log.error(" Failure: " + key + " :: " + results[key])
            return

        # tracking_record = self.initialize_tracking(project_id, experiment_name, experiment_description,
        #          globus_endpoint, endpoint_path, request_uuid,
        #          excel_file_relative_path, data_dir_relitive_path)
        #
        # self.submit_globus_transfer(project_id, globus_endpoint, endpoint_path)
        else:
            results = self.globus_transfer(project_id, globus_endpoint, endpoint_path)
            self.log.info(results)
            if results['status'] == 'SUCCEEDED':
                self.build_experiment(project_id, experiment_name, experiment_description,
                                      excel_file_path, data_file_path)

    def verify_preconditions(self, project_id,
                 globus_endpoint, endpoint_path, base_path,
                 excel_file_relative_path, data_dir_relitive_path):
        web_service = MaterialsCommonsGlobusInterface(self.user_id)
        checker = VerifySetup(web_service, project_id,
                 globus_endpoint, endpoint_path, base_path,
                 excel_file_relative_path, data_dir_relitive_path)
        return checker.status()

    def globus_transfer(self, project_id, globus_endpoint, endpoint_path):
        web_service = MaterialsCommonsGlobusInterface(self.user_id)
        self.log.info("set_transfer_client")
        results = web_service.set_transfer_client()
        if results['status'] == 'error':
            return results

        self.log.info("stage_upload_files")
        results = web_service.stage_upload_files(project_id, globus_endpoint, endpoint_path)
        self.log.info("results of staging: ", results)
        task_id = results['task_id']
        poll = True
        while poll:
            results = web_service.get_task_status(task_id)
            poll = (results['status'] == 'ACTIVE')
        self.log.info(results)
        return results

    @staticmethod
    def build_experiment(project_id, experiment_name, experiment_description,
                         excel_file_path, data_file_path):
        builder = BuildProjectExperiment()
        builder.set_rename_is_ok(True)
        builder.preset_project_id(project_id)
        builder.preset_experiment_name_description(experiment_name, experiment_description)
        builder.build(excel_file_path, data_file_path)
