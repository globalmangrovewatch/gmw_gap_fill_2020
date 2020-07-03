from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import sys
import rsgislib.imageutils
import rsgislib.imageutils.imagecomp

sys.path.insert(0, "../03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class ComputeSen2GranuleComposite(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='comp_composite.py', descript=None)

    def do_processing(self, **kwargs):
        n_imgs = len(self.params['imgs'])

        if n_imgs > 1:
            rBand = 3
            nBand = 7
            sBand = 9

            outRefImg = os.path.join(self.params['comp_dir'], "sen2_comp_{}_refimg.kea".format(self.params['granule']))
            outCompImg = os.path.join(self.params['comp_dir'], "sen2_comp_{}_refl.kea".format(self.params['granule']))
            outMskImg = os.path.join(self.params['comp_dir'], "sen2_comp_{}_mskimg.kea".format(self.params['granule']))

            rsgislib.imageutils.imagecomp.createMaxNDVINDWIComposite(self.params['imgs'][0], self.params['imgs'],
                                                                     rBand, nBand, sBand, outRefImg,
                                                                     outCompImg, outMskImg, tmpPath=self.params['tmp_dir'],
                                                                     gdalformat='KEA', dataType=None, calcStats=True,
                                                                     reprojmethod='cubic', use_mode=True)
            outCompTIFImg = os.path.join(self.params['comp_tif_dir'], "sen2_comp_{}_refl.tif".format(self.params['granule']))
            rsgislib.imageutils.gdal_translate(outCompImg, outCompTIFImg, gdal_format='GTIFF')
        elif n_imgs == 1:
            outCompTIFImg = os.path.join(self.params['comp_tif_dir'], "sen2_comp_{}_refl.tif".format(self.params['granule']))
            rsgislib.imageutils.gdal_translate(self.params['imgs'][0], outCompTIFImg, gdal_format='GTIFF')


        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["granule", "imgs", "comp_dir", "comp_tif_dir", "tmp_dir"]

    def outputs_present(self, **kwargs):
        #outCompTIFImg = os.path.join(self.params['comp_tif_dir'], "sen2_comp_{}_refl.tif".format(self.params['granule']))

        return True, dict()

if __name__ == "__main__":
    ComputeSen2GranuleComposite().std_run()


