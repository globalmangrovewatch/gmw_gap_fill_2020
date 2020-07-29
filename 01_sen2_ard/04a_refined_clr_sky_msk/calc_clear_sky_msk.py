from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib.imagecalibration

logger = logging.getLogger(__name__)


class ComputeSen2ClearSkyMask(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='calc_clear_sky_msk.py', descript=None)

    def do_processing(self, **kwargs):

        rsgislib.imagecalibration.calcClearSkyRegions(self.params['cloud_msk'], self.params['valid_msk'],
                                                      self.params['out_clrsky_img'], 'KEA', self.params['tmp_dir'],
                                                      deleteTmpFiles=True, initClearSkyRegionDist=1000,
                                                      initClearSkyRegionMinSize=1000, finalClearSkyRegionDist=500,
                                                      morphSize=11)
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["product_id", "cloud_msk", "valid_msk", "out_clrsky_img", "tmp_dir"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_clrsky_img']] = 'gdal_image'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

        # Remove the output file.
        if os.path.exists(self.params['out_clrsky_img']):
            os.remove(self.params['out_clrsky_img'])

if __name__ == "__main__":
    ComputeSen2ClearSkyMask().std_run()


