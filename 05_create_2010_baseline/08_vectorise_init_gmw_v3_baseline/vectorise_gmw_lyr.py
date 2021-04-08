from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil

logger = logging.getLogger(__name__)


class VectoriseGMWLyr(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='vectorise_gmw_lyr.py', descript=None)

    def do_processing(self, **kwargs):
        import rsgislib.vectorutils

        rsgislib.vectorutils.polygoniseRaster2VecLyr(self.params['out_file'], "gmw_v3_init", 'GPKG',
                                                     self.params['gmw_v3_img'], imgBandNo=1,
                                                     maskImg=self.params['gmw_v3_img'],
                                                     imgMaskBandNo=1, replace_file=True, replace_lyr=True,
                                                     pxl_val_fieldname='PXLVAL')


    def required_fields(self, **kwargs):
        return ["tile_img", "gmw_v3_img", "out_file"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_file']] = 'gdal_vector'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output file.
        if os.path.exists(self.params['out_file']):
            os.remove(self.params['out_file'])


if __name__ == "__main__":
    VectoriseGMWLyr().std_run()


