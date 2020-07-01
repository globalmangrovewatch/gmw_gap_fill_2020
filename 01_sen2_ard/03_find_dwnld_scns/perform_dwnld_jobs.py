from pbprocesstools.pbpt_q_process import PBPTQProcessTool
from sen2scnprocess import RecordSen2Process
import logging
import subprocess

logger = logging.getLogger(__name__)

class PerformScnDownload(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='exe_scn_processing.py', descript=None)

    def do_processing(self, **kwargs):
        sen2_rcd_obj = RecordSen2Process(self.params['scn_db_file'])
        downloaded = sen2_rcd_obj.is_scn_downloaded(self.params['product_id'])
        if not downloaded:
            auth_cmd = "gcloud auth activate-service-account --key-file={}".format(self.params['goog_key_json'])
            cmd = "gsutil -m cp -r {} {}".format(self.params['scn_url'], self.params['downpath'])
            logger.debug("Running command: '{}'".format(auth_cmd))
            subprocess.call(auth_cmd, shell=True)
            logger.debug("Running command: '{}'".format(cmd))
            subprocess.call(cmd, shell=True)
            sen2_rcd_obj.set_scn_downloaded(self.params['product_id'], self.params['downpath'])

    def required_fields(self, **kwargs):
        return ["product_id", "scn_url", "downpath", "scn_db_file", "goog_key_json"]

    def outputs_present(self, **kwargs):
        sen2_rcd_obj = RecordSen2Process(self.params['scn_db_file'])
        downloaded = sen2_rcd_obj.is_scn_downloaded(self.params['product_id'])
        return downloaded, dict()

if __name__ == "__main__":
    PerformScnDownload().std_run()


