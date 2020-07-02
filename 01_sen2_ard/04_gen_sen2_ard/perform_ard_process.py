from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import arcsilib
import arcsilib.arcsirun
import os
import shutil
import sys

sys.path.insert(0, "../03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class PerformScnARD(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='perform_ard_process.py', descript=None)

    def find_first_file(self, dirPath, fileSearch):
        """
        Search for a single file with a path using glob. Therefore, the file
        path returned is a true path. Within the fileSearch provide the file
        name with '*' as wildcard(s).
        :param dirPath:
        :param fileSearch:
        :return:
        """
        import glob
        for root, dirs, files in os.walk(dirPath):
            files = glob.glob(os.path.join(root, fileSearch))
            if len(files) > 0:
                break

        if len(files) != 1:
            raise Exception("Could not find a single file ({0}) in {1}; found {2} files.".format(fileSearch, dirPath, len(files)))
        return files[0]

    def do_processing(self, **kwargs):
        sen2_rcd_obj = RecordSen2Process(self.params['scn_db_file'])
        downloaded = sen2_rcd_obj.is_scn_downloaded(self.params['product_id'])
        ard_processed = sen2_rcd_obj.is_scn_ard(self.params['product_id'])
        if downloaded and (not ard_processed):
            input_hdr = self.find_first_file(self.params['dwnld_path'], "*MTD*.xml")

            arcsilib.arcsirun.runARCSI(input_hdr, None, None, "sen2", None, "KEA",
                                       self.params['ard_path'], None, None, None, None, None, None,
                                       ["CLOUDS", "CLEARSKY", "DOSAOTSGL", "STDSREF", "SATURATE", "TOPOSHADOW",
                                        "METADATA"],
                                       True, None, None, arcsilib.DEFAULT_ARCSI_AEROIMG_PATH,
                                       arcsilib.DEFAULT_ARCSI_ATMOSIMG_PATH,
                                       "GreenVegetation", 0, None, None, False, None, None, None, None, False,
                                       None, None, self.params['tmp_dir'], 0.05, 0.5, 0.1, 0.4, self.params['dem'],
                                       None, None, True, 20, False, False, 1000, "cubic", "near", 3000, 3000, 1000, 21,
                                       True, False, False, None, None, True, None, 'S2LESSFMSK')
            sen2_rcd_obj.set_scn_ard(self.params['product_id'], self.params['ard_path'])
        elif ard_processed:
            sen2_rcd_obj.set_scn_ard(self.params['product_id'], self.params['ard_path'])

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["product_id", "dwnld_path", "ard_path", "scn_db_file", "tmp_dir", "dem"]

    def outputs_present(self, **kwargs):
        sen2_rcd_obj = RecordSen2Process(self.params['scn_db_file'])
        ard_processed = sen2_rcd_obj.is_scn_ard(self.params['product_id'])
        return ard_processed, dict()

if __name__ == "__main__":
    PerformScnARD().std_run()


