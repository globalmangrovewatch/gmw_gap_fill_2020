from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import rsgislib
import rsgislib.vectorutils

logger = logging.getLogger(__name__)


class RasteriseGMW2010(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='rasterise_gmw2010.py', descript=None)

    def do_processing(self, **kwargs):

        rsgislib.vectorutils.rasteriseVecLyr(self.params['gmw_vec'], self.params['gmw2010_lyr'],
                                             self.params['tile_img'], self.params['out_file'], gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

    def required_fields(self, **kwargs):
        return ["tile_img", "gmw_vec", "gmw2010_lyr", "out_file"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_file']] = 'gdal_image'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output file.
        if os.path.exists(self.params['out_file']):
            os.remove(self.params['out_file'])

if __name__ == "__main__":
    RasteriseGMW2010().std_run()


