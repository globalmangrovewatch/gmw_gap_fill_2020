from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib
import rsgislib.imagecalc
import rsgislib.rastergis

logger = logging.getLogger(__name__)


class MergeGMWEdits(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='merge_gmw_edits.py', descript=None)

    def do_processing(self, **kwargs):

        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('qa', self.params['qa_xtrs_img'], 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('gfill', self.params['gapfill_img'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_file'], '(qa==1)||(gfill==1)?1:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)
        rsgislib.rastergis.populateStats(self.params['out_file'], addclrtab=True, calcpyramids=True, ignorezero=True)


    def required_fields(self, **kwargs):
        return ["tile_img", "qa_xtrs_img", "gapfill_img", "out_file"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_file']] = 'gdal_image'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output file.
        if os.path.exists(self.params['out_file']):
            os.remove(self.params['out_file'])

if __name__ == "__main__":
    MergeGMWEdits().std_run()


