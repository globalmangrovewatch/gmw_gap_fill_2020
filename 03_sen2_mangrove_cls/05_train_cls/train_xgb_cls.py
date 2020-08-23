from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import rsgislib
import rsgislib.classification
import rsgislib.classification.classxgboost

logger = logging.getLogger(__name__)


class TrainXGBParams(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='train_xgb_cls.py', descript=None)

    def do_processing(self, **kwargs):
        rsgislib.classification.classxgboost.train_xgboost_binary_classifer(self.params['out_cls_file'],
                                                                            self.params['cls_params_file'],
                                                                            self.params['mng_train_smps_file'],
                                                                            self.params['mng_valid_smps_file'],
                                                                            self.params['mng_test_smps_file'],
                                                                            self.params['oth_train_smps_file'],
                                                                            self.params['oth_valid_smps_file'],
                                                                            self.params['oth_test_smps_file'],
                                                                            nthread=1, mdl_cls_obj=None)

    def required_fields(self, **kwargs):
        return ["mng_train_smps_file", "mng_valid_smps_file", "mng_test_smps_file",
                "oth_train_smps_file", "oth_valid_smps_file", "oth_test_smps_file",
                "cls_params_file", "out_cls_file"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_cls_file']] = 'file'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output file.
        if os.path.exists(self.params['out_cls_file']):
            os.remove(self.params['out_cls_file'])

if __name__ == "__main__":
    TrainXGBParams().std_run()

