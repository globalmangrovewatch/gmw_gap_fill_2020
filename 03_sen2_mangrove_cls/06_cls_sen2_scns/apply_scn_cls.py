from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib
import rsgislib.imageutils
import rsgislib.classification.classxgboost

logger = logging.getLogger(__name__)


class ApplyXGBClass(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='apply_scn_cls.py', descript=None)

    def do_processing(self, **kwargs):
        if not os.path.exists(self.params['tmp_dir']):
            os.mkdir(self.params['tmp_dir'])

        fileInfo = [rsgislib.imageutils.ImageBandInfo(self.params['sref_img'], 'sen2', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]
        outProbImg = os.path.join(self.params['tmp_dir'], "prob_cls_img.kea")
        rsgislib.classification.classxgboost.apply_xgboost_binary_classifier(self.params['cls_mdl_file'],
                                                                             self.params['clrsky_img'], 1,
                                                                             fileInfo, outProbImg, 'KEA',
                                                                             outClassImg=self.params['out_cls_file'],
                                                                             class_thres=5000, nthread=1)
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["scn_id", "vld_img", "clrsky_img", "sref_img", "cls_mdl_file", "out_cls_file", "tmp_dir"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_cls_file']] = 'file'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

        # Remove the output file.
        if os.path.exists(self.params['out_cls_file']):
            os.remove(self.params['out_cls_file'])

if __name__ == "__main__":
    ApplyXGBClass().std_run()


