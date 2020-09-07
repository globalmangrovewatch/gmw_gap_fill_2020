from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os

logger = logging.getLogger(__name__)

class VectoriseCls(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='vectorise_granule_cls.py', descript=None)

    def do_processing(self, **kwargs):
        import rsgislib.vectorutils

        rsgislib.vectorutils.polygoniseRaster2VecLyr(self.params['cls_vec_file'], "mangroves", 'GPKG',
                                                     self.params['cls_img_file'], imgBandNo=1,
                                                     maskImg=self.params['cls_img_file'],
                                                     imgMaskBandNo=1, replace_file=True, replace_lyr=True,
                                                     pxl_val_fieldname='PXLVAL')


    def required_fields(self, **kwargs):
        return ["granule", "cls_img_file", "cls_vec_file"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['cls_vec_file']] = 'gdal_vector'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output files.
        if os.path.exists(self.params['cls_vec_file']):
            os.remove(self.params['cls_vec_file'])

if __name__ == "__main__":
    VectoriseCls().std_run()


