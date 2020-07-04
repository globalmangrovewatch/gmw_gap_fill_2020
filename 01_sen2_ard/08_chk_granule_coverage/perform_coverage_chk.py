from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib
import rsgislib.imagecalc
import rsgislib.vectorutils
import rsgislib.rastergis
import rsgislib.rastergis.ratutils

logger = logging.getLogger(__name__)

class PerformCoverageCheck(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='perform_coverage_chk.py', descript=None)

    def do_processing(self, **kwargs):
        if len(self.params['vld_imgs']) != len(self.params['clrsky_imgs']):
            raise Exception("There are different numbers of valid masks and clear sky masks.")

        rsgis_utils = rsgislib.RSGISPyUtils()
        granule_epsg = rsgis_utils.getEPSGCode(self.params['vld_imgs'][0])
        # Re-project vector layer
        granule_vec_file = os.path.join(self.params['tmp_dir'], "{}_roi_lcl_prj.geojson".format(self.params['granule']))
        rsgislib.vectorutils.vector_translate(self.params['roi_vec_file'], self.params['roi_vec_lyr'], granule_vec_file, self.params['roi_vec_lyr'], out_vec_drv='GEOJSON',
                         drv_create_opts=['-skipfailures'], lyr_create_opts=['-skipfailures'], access_mode='overwrite', src_srs="EPSG:4326",
                         dst_srs="EPSG:{}".format(granule_epsg), force=True)

        # Rasterise
        granule_roi_img = os.path.join(self.params['tmp_dir'], "{}_roi_img.kea".format(self.params['granule']))
        rsgislib.vectorutils.rasteriseVecLyr(granule_vec_file, self.params['roi_vec_lyr'], self.params['vld_imgs'][0], granule_roi_img, gdalformat="KEA", burnVal=1, datatype=rsgislib.TYPE_8UINT)

        granule_vld_img = os.path.join(self.params['tmp_dir'], "{}_vld_img.kea".format(self.params['granule']))
        rsgislib.imagecalc.calcMultiImgBandStats(self.params['vld_imgs'], granule_vld_img, rsgislib.SUMTYPE_MAX, "KEA", rsgislib.TYPE_8UINT, 0, False)

        granule_clearsky_img = os.path.join(self.params['tmp_dir'], "{}_clearsky_img.kea".format(self.params['granule']))
        rsgislib.imagecalc.calcMultiImgBandStats(self.params['clrsky_imgs'], granule_clearsky_img, rsgislib.SUMTYPE_MAX, "KEA", rsgislib.TYPE_8UINT, 0, False)

        band_defns = []
        band_defns.append(rsgislib.imagecalc.BandDefn('roi', granule_roi_img, 1))
        band_defns.append(rsgislib.imagecalc.BandDefn('vld', granule_vld_img, 1))
        granule_vld_roi_img = os.path.join(self.params['tmp_dir'], "{}_vld_roi_img.kea".format(self.params['granule']))
        rsgislib.imagecalc.bandMath(granule_vld_roi_img, '(roi==1)&&(vld==1)?1:0', 'KEA', rsgislib.TYPE_8UINT, band_defns)
        rsgislib.rastergis.populateStats(granule_vld_roi_img, addclrtab=True, calcpyramids=True, ignorezero=True)

        band_defns = []
        band_defns.append(rsgislib.imagecalc.BandDefn('roi', granule_roi_img, 1))
        band_defns.append(rsgislib.imagecalc.BandDefn('clr', granule_clearsky_img, 1))
        granule_clearsky_roi_img = os.path.join(self.params['tmp_dir'], "{}_clearsky_roi_img.kea".format(self.params['granule']))
        rsgislib.imagecalc.bandMath(granule_clearsky_roi_img, '(roi==1)&&(clr==1)?1:0', 'KEA', rsgislib.TYPE_8UINT, band_defns)
        rsgislib.rastergis.populateStats(granule_clearsky_roi_img, addclrtab=True, calcpyramids=True, ignorezero=True)

        # Get stats
        # N ROI pixels
        roi_hist = rsgislib.rastergis.ratutils.getColumnData(granule_roi_img, 'Histogram')
        if len(roi_hist) > 1:
            n_roi_pxls = roi_hist[1]
        else:
            n_roi_pxls = 0
        # N Valid ROI pixels
        vld_roi_hist = rsgislib.rastergis.ratutils.getColumnData(granule_vld_roi_img, 'Histogram')
        if len(vld_roi_hist) > 1:
            n_vld_roi_pxls = vld_roi_hist[1]
        else:
            n_vld_roi_pxls = 0
        # N ClearSky ROI pixels
        clrsky_roi_hist = rsgislib.rastergis.ratutils.getColumnData(granule_clearsky_roi_img, 'Histogram')
        if len(clrsky_roi_hist) > 1:
            n_clrsky_roi_pxls = clrsky_roi_hist[1]
        else:
            n_clrsky_roi_pxls = 0

        out_info = dict()
        out_info['n_roi_pxls'] = n_roi_pxls
        out_info['n_vld_roi_pxls'] = n_vld_roi_pxls
        out_info['n_clrsky_roi_pxls'] = n_clrsky_roi_pxls

        rsgis_utils.writeDict2JSON(out_info, self.params['granule_out_file'])

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["granule", "vld_imgs", "clrsky_imgs", "roi_vec_file", "roi_vec_lyr", "granule_out_file", "tmp_dir"]

    def outputs_present(self, **kwargs):
        return os.path.exists(self.params['granule_out_file']), dict()

if __name__ == "__main__":
    PerformCoverageCheck().std_run()


