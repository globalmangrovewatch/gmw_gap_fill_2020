from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib
import rsgislib.imagecalc
import rsgislib.vectorutils
import rsgislib.rastergis
import osgeo.ogr as ogr

logger = logging.getLogger(__name__)

class CreateGranuleValidMsk(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='perform_coverage_chk.py', descript=None)

    def do_processing(self, **kwargs):
        if len(self.params['vld_imgs']) != len(self.params['clrsky_imgs']):
            raise Exception("There are different numbers of valid masks and clear sky masks.")

        granule_vld_img = os.path.join(self.params['tmp_dir'], "{}_vld_img.kea".format(self.params['granule']))
        rsgislib.imagecalc.calcMultiImgBandStats(self.params['vld_imgs'], granule_vld_img, rsgislib.SUMTYPE_MAX, "KEA", rsgislib.TYPE_8UINT, 0, False)

        granule_clearsky_img = os.path.join(self.params['tmp_dir'], "{}_clearsky_img.kea".format(self.params['granule']))
        rsgislib.imagecalc.calcMultiImgBandStats(self.params['clrsky_imgs'], granule_clearsky_img, rsgislib.SUMTYPE_MAX, "KEA", rsgislib.TYPE_8UINT, 0, False)

        band_defns = []
        band_defns.append(rsgislib.imagecalc.BandDefn('clr', granule_clearsky_img, 1))
        band_defns.append(rsgislib.imagecalc.BandDefn('vld', granule_vld_img, 1))
        granule_vld_clr_img = os.path.join(self.params['tmp_dir'], "{}_vld_roi_img.kea".format(self.params['granule']))
        rsgislib.imagecalc.bandMath(granule_vld_clr_img, '(clr==1)&&(vld==1)?1:0', 'KEA', rsgislib.TYPE_8UINT, band_defns)
        rsgislib.rastergis.populateStats(granule_vld_clr_img, addclrtab=True, calcpyramids=True, ignorezero=True)

        rsgislib.vectorutils.polygoniseRaster2VecLyr(self.params['granule_out_file'], self.params['granule_out_lyr'], 'GPKG', granule_vld_clr_img, imgBandNo=1, maskImg=granule_vld_clr_img, imgMaskBandNo=1, replace_file=True, replace_lyr=True, pxl_val_fieldname='PXLVAL')

        n_feats = rsgislib.vectorutils.getVecFeatCount(self.params['granule_out_file'], self.params['granule_out_lyr'])
        granule_names = list()
        for i in range(n_feats):
            granule_names.append(self.params['granule'])
        rsgislib.vectorutils.writeVecColumn(self.params['granule_out_file'], self.params['granule_out_lyr'], 'Granule', ogr.OFTString, granule_names)

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["granule", "vld_imgs", "clrsky_imgs", "granule_out_lyr", "granule_out_file", "tmp_dir"]

    def outputs_present(self, **kwargs):
        return os.path.exists(self.params['granule_out_file']), dict()

    def remove_outputs(self, **kwargs):
        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

        # Remove the output file.
        if os.path.exists(self.params['granule_out_file']):
            os.remove(self.params['granule_out_file'])

if __name__ == "__main__":
    CreateGranuleValidMsk().std_run()


