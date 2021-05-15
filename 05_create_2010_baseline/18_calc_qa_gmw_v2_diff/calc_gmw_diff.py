from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib
import rsgislib.imagecalc
import rsgislib.rastergis

logger = logging.getLogger(__name__)


class CalcGMWDiff(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='calc_gmw_diff.py', descript=None)

    def do_processing(self, **kwargs):

        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('gmw_v2', self.params['gmw_v2_img'], 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('gmw_v3', self.params['gmw_v3_img'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_add_file'], '(gmw_v2==0)&&(gmw_v3==1)?1:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)
        rsgislib.rastergis.populateStats(self.params['out_add_file'], addclrtab=True, calcpyramids=True, ignorezero=True)

        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('gmw_v2', self.params['gmw_v2_img'], 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('gmw_v3', self.params['gmw_v3_img'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_rmv_file'], '(gmw_v2==1)&&(gmw_v3==0)?1:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)
        rsgislib.rastergis.populateStats(self.params['out_rmv_file'], addclrtab=True, calcpyramids=True, ignorezero=True)


    def required_fields(self, **kwargs):
        return ["tile_img", "gmw_v2_img", "gmw_v3_img", "out_add_file", "out_rmv_file"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_add_file']] = 'gdal_image'
        files_dict[self.params['out_rmv_file']] = 'gdal_image'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output file.
        if os.path.exists(self.params['out_add_file']):
            os.remove(self.params['out_add_file'])

        if os.path.exists(self.params['out_rmv_file']):
            os.remove(self.params['out_rmv_file'])

if __name__ == "__main__":
    CalcGMWDiff().std_run()


