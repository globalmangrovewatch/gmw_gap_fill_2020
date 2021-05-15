from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib
import rsgislib.imagecalc
import rsgislib.rastergis
import rsgislib.vectorutils

logger = logging.getLogger(__name__)


class ApplySundarbansEdits(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='apply_sundarbans_edits.py', descript=None)

    def do_processing(self, **kwargs):
        if not os.path.exists(self.params['tmp_dir']):
            os.mkdir(self.params['tmp_dir'])

        roi_img = os.path.join(self.params['tmp_dir'], "{}_roi_img.kea".format(self.params['tile_basename']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['sundarbans_roi_vec_file'], self.params['sundarbans_roi_vec_lyr'],
                                             self.params['tile_img'], roi_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        sundarbans_mng_img = os.path.join(self.params['tmp_dir'], "{}_sundarbans_mng_img.kea".format(self.params['tile_basename']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['sundarbans_vec_file'], self.params['sundarbans_vec_lyr'],
                                             self.params['tile_img'], sundarbans_mng_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('roi', roi_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('mng', sundarbans_mng_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('gmw', self.params['gmw_v3_img'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_file'], '(roi==1)?mng:gmw', 'KEA', rsgislib.TYPE_8UINT, bandDefns)
        rsgislib.rastergis.populateStats(self.params['out_file'], addclrtab=True, calcpyramids=True, ignorezero=True)

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])


    def required_fields(self, **kwargs):
        return ["tile_basename", "tile_img", "gmw_v3_img", "sundarbans_vec_file", "sundarbans_vec_lyr", "sundarbans_roi_vec_file", "sundarbans_roi_vec_lyr", "out_file", "tmp_dir"]


    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_file']] = 'gdal_image'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output file.
        if os.path.exists(self.params['out_file']):
            os.remove(self.params['out_file'])

        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

if __name__ == "__main__":
    ApplySundarbansEdits().std_run()


