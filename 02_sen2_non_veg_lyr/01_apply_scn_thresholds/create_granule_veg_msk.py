from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib
import rsgislib.imagecalc
import rsgislib.vectorutils
import rsgislib.rastergis
import rsgislib.imagecalc.calcindices
import rsgislib.imageutils

import osgeo.ogr as ogr

logger = logging.getLogger(__name__)

class CreateGranuleVegMsk(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='perform_coverage_chk.py', descript=None)

    def do_processing(self, **kwargs):
        if (len(self.params['vld_imgs']) != len(self.params['clrsky_imgs'])) and (len(self.params['vld_imgs']) != len(self.params['sref_imgs'])):
            raise Exception("There are different numbers of valid masks, clear sky masks and SREF images.")
        rsgis_utils = rsgislib.RSGISPyUtils()

        if len(self.params['vld_imgs']) > 0:
            granule_vld_img = os.path.join(self.params['tmp_dir'], "{}_vld_img.kea".format(self.params['granule']))
            rsgislib.imagecalc.calcMultiImgBandStats(self.params['vld_imgs'], granule_vld_img, rsgislib.SUMTYPE_MAX,
                                                     "KEA", rsgislib.TYPE_8UINT, 0, False)

            granule_dem_img = os.path.join(self.params['tmp_dir'], "{}_dem.kea".format(self.params['granule']))
            rsgislib.imageutils.resampleImage2Match(granule_vld_img, self.params['dem_file'], granule_dem_img, 'KEA', 'cubicspline', rsgislib.TYPE_32FLOAT, noDataVal=None, multicore=False)

            scn_nonveg_msks = list()
            for sref_img in self.params['sref_imgs']:
                print(sref_img)
                basename = rsgis_utils.get_file_basename(sref_img)
                scn_ndvi_img = os.path.join(self.params['tmp_dir'], "{}_ndvi.kea".format(basename))

                rBand = 3
                nBand = 7
                rsgislib.imagecalc.calcindices.calcNDVI(sref_img, rBand, nBand, scn_ndvi_img, stats=False, gdalformat='KEA')

                scn_nonveg_img = os.path.join(self.params['tmp_dir'], "{}_nonveg.kea".format(basename))
                band_defs = [rsgislib.imagecalc.BandDefn('ndvi', scn_ndvi_img, 1),
                             rsgislib.imagecalc.BandDefn('dem', granule_dem_img, 1)]
                exp = '(dem>80) || (ndvi<0.2)?1:0'
                rsgislib.imagecalc.bandMath(scn_nonveg_img, exp, 'KEA', rsgislib.TYPE_8UINT, band_defs)
                rsgislib.rastergis.populateStats(scn_nonveg_img, addclrtab=True, calcpyramids=True, ignorezero=True)
                scn_nonveg_msks.append(scn_nonveg_img)

            granule_nonveg_img = os.path.join(self.params['tmp_dir'], "{}_nonveg_img.kea".format(self.params['granule']))
            rsgislib.imagecalc.calcMultiImgBandStats(scn_nonveg_msks, granule_nonveg_img, rsgislib.SUMTYPE_MAX, "KEA", rsgislib.TYPE_8UINT, 0, False)
            rsgislib.rastergis.populateStats(granule_nonveg_img, addclrtab=True, calcpyramids=True, ignorezero=True)

            band_defs = [rsgislib.imagecalc.BandDefn('nveg', granule_nonveg_img, 1),
                         rsgislib.imagecalc.BandDefn('vld', granule_vld_img, 1)]
            exp = '(vld==1) && (nveg==0)?1:0'
            rsgislib.imagecalc.bandMath(self.params['granule_out_img_file'], exp, 'KEA', rsgislib.TYPE_8UINT, band_defs)
            rsgislib.rastergis.populateStats(self.params['granule_out_img_file'], addclrtab=True, calcpyramids=True, ignorezero=True)


            rsgislib.vectorutils.polygoniseRaster2VecLyr(self.params['granule_out_vec_file'], self.params['granule_out_lyr'], 'GPKG', self.params['granule_out_img_file'], imgBandNo=1, maskImg=self.params['granule_out_img_file'], imgMaskBandNo=1, replace_file=True, replace_lyr=True, pxl_val_fieldname='PXLVAL')

            n_feats = rsgislib.vectorutils.getVecFeatCount(self.params['granule_out_vec_file'], self.params['granule_out_lyr'])
            granule_names = list()
            for i in range(n_feats):
                granule_names.append(self.params['granule'])
            rsgislib.vectorutils.writeVecColumn(self.params['granule_out_file'], self.params['granule_out_lyr'], 'Granule', ogr.OFTString, granule_names)

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["granule", "dem_file", "vld_imgs", "clrsky_imgs", "sref_imgs", "granule_out_lyr", "granule_out_vec_file", "granule_out_img_file", "tmp_dir"]

    def outputs_present(self, **kwargs):
        return os.path.exists(self.params['granule_out_file']), dict()

if __name__ == "__main__":
    CreateGranuleVegMsk().std_run()

