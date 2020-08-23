from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import time
import rsgislib
import rsgislib.imageutils

logger = logging.getLogger(__name__)

class MergeScnCls(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='merge_scn_cls.py', descript=None)

    def do_processing(self, **kwargs):
        import rsgislib
        import rsgislib.imagecalc
        import rsgislib.imageutils
        import rsgislib.rastergis

        try:
            rsgislib.imagecalc.calcMultiImgBandStats(self.params['cls_files'], self.params['out_sum_cls_file'],
                                                     rsgislib.SUMTYPE_MEAN, 'KEA', rsgislib.TYPE_32FLOAT, 0, False)
        except Exception as e:
            rsgis_utils = rsgislib.RSGISPyUtils()
            for img in self.params['cls_files']:
                print(img)
                print(rsgis_utils.getImageRes(img))
            print(e)
            raise e
        rsgislib.imageutils.popImageStats(self.params['out_sum_cls_file'], usenodataval=True, nodataval=0, calcpyramids=True)

        band_defns = list()
        band_defns.append(rsgislib.imagecalc.BandDefn('cld', self.params['clr_sky'], 1))
        band_defns.append(rsgislib.imagecalc.BandDefn('scr', self.params['out_sum_cls_file'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_cls_25_file'], "cld>0?255:scr>0.3?1:0", 'KEA', rsgislib.TYPE_8UINT, band_defns)
        rsgislib.rastergis.populateStats(self.params['out_cls_25_file'], addclrtab=True, calcpyramids=True, ignorezero=False)

        band_defns = list()
        band_defns.append(rsgislib.imagecalc.BandDefn('cld', self.params['clr_sky'], 1))
        band_defns.append(rsgislib.imagecalc.BandDefn('scr', self.params['out_sum_cls_file'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_cls_50_file'], "cld>0?255:scr>0.5?1:0", 'KEA', rsgislib.TYPE_8UINT, band_defns)
        rsgislib.rastergis.populateStats(self.params['out_cls_50_file'], addclrtab=True, calcpyramids=True, ignorezero=False)

        band_defns = list()
        band_defns.append(rsgislib.imagecalc.BandDefn('cld', self.params['clr_sky'], 1))
        band_defns.append(rsgislib.imagecalc.BandDefn('scr', self.params['out_sum_cls_file'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_cls_75_file'], "cld>0?255:scr>0.8?1:0", 'KEA', rsgislib.TYPE_8UINT, band_defns)
        rsgislib.rastergis.populateStats(self.params['out_cls_75_file'], addclrtab=True, calcpyramids=True, ignorezero=False)


    def required_fields(self, **kwargs):
        return ["scn_id", "cls_files", "clr_sky", "out_sum_cls_file", "out_cls_25_file", "out_cls_50_file", "out_cls_75_file"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_sum_cls_file']] = 'gdal_img'
        files_dict[self.params['out_cls_25_file']] = 'gdal_img'
        files_dict[self.params['out_cls_50_file']] = 'gdal_img'
        files_dict[self.params['out_cls_75_file']] = 'gdal_img'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output files.
        if os.path.exists(self.params['out_sum_cls_file']):
            os.remove(self.params['out_sum_cls_file'])

        if os.path.exists(self.params['out_cls_25_file']):
            os.remove(self.params['out_cls_25_file'])

        if os.path.exists(self.params['out_cls_50_file']):
            os.remove(self.params['out_cls_50_file'])

        if os.path.exists(self.params['out_cls_75_file']):
            os.remove(self.params['out_cls_75_file'])

if __name__ == "__main__":
    MergeScnCls().std_run()


