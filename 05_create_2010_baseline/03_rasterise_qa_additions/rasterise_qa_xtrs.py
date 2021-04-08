from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc

logger = logging.getLogger(__name__)


class RasteriseQAExtras(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='rasterise_qa_xtrs.py', descript=None)

    def do_processing(self, **kwargs):

        gapfill_qa_img = os.path.join(self.params['tmp_dir'], 'gapfill_qa.kea')
        rsgislib.vectorutils.rasteriseVecLyr(self.params['gapfill_qa_vec'], self.params['gapfill_qa_lyr'],
                                             self.params['tile_img'], gapfill_qa_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        french_img = os.path.join(self.params['tmp_dir'], 'french_mangroves.kea')
        rsgislib.vectorutils.rasteriseVecLyr(self.params['french_qa_vec'], self.params['french_qa_lyr'],
                                             self.params['tile_img'], french_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('qa', gapfill_qa_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('french', french_img, 1))
        rsgislib.imagecalc.bandMath(self.params['out_file'], '(qa==1)||(french==1)?1:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["tile_img", "gapfill_qa_vec", "gapfill_qa_lyr", "french_qa_vec", "french_qa_lyr", "out_file", "tmp_dir"]

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
    RasteriseQAExtras().std_run()


