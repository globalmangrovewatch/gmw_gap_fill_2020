from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import pathlib

logger = logging.getLogger(__name__)


class VectoriseGMWLyr(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='vectorise_gmw_lyr.py', descript=None)

    def do_processing(self, **kwargs):
        import rsgislib.vectorutils
        import rsgislib.imagecalc

        pxl_count = rsgislib.imagecalc.countPxlsOfVal(self.params['gmw_v3_img'], vals=[1])

        if pxl_count[0] > 0:
            rsgislib.vectorutils.polygoniseRaster2VecLyr(self.params['out_file'], "gmw_v3_init", 'GPKG',
                                                         self.params['gmw_v3_img'], imgBandNo=1,
                                                         maskImg=self.params['gmw_v3_img'],
                                                         imgMaskBandNo=1, replace_file=True, replace_lyr=True,
                                                         pxl_val_fieldname='PXLVAL')

        pathlib.Path(self.params['out_cmp_file']).touch()


    def required_fields(self, **kwargs):
        return ["tile_img", "gmw_v3_img", "out_file", "out_cmp_file"]

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


