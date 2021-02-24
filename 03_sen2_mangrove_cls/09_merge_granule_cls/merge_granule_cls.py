from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os

logger = logging.getLogger(__name__)

class MergeGranuleCls(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='merge_granule_cls.py', descript=None)

    def do_processing(self, **kwargs):
        import rsgislib
        import rsgislib.imagecalc
        import rsgislib.imageutils
        import rsgislib.rastergis
        import h5py

        rsgis_utils = rsgislib.RSGISPyUtils()

        cls_files = []
        for cls_file in self.params['cls_files']:
            if os.path.exists(cls_file):
                try:
                    print(cls_file)
                    test_f = h5py.File(cls_file, 'r')
                    kea_img_items = list(test_f.keys())
                    test_f.close()

                    n_bands = rsgis_utils.getImageBandCount(cls_file)
                    if n_bands == 1:
                        cls_files.append(cls_file)
                except:
                    print("REMOVING: {}".format(cls_file))
                    os.remove(cls_file)

        try:
            rsgislib.imagecalc.calcMultiImgBandStats(cls_files, self.params['out_sum_cls_file'],
                                                     rsgislib.SUMTYPE_MEAN, 'KEA', rsgislib.TYPE_32FLOAT, 0, True)
        except Exception as e:
            for img in cls_files:
                print(img)
                print(rsgis_utils.getImageRes(img))
                print(rsgis_utils.getImageBandCount(img))
            print(e)
            raise e
        rsgislib.imageutils.popImageStats(self.params['out_sum_cls_file'], usenodataval=True, nodataval=0, calcpyramids=True)

        band_defns = list()
        band_defns.append(rsgislib.imagecalc.BandDefn('scr', self.params['out_sum_cls_file'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_cls_50_file'], "scr>0.5?1:0", 'KEA', rsgislib.TYPE_8UINT, band_defns)
        rsgislib.rastergis.populateStats(self.params['out_cls_50_file'], addclrtab=True, calcpyramids=True, ignorezero=False)

        band_defns = list()
        band_defns.append(rsgislib.imagecalc.BandDefn('scr', self.params['out_sum_cls_file'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_cls_75_file'], "scr>0.75?1:0", 'KEA', rsgislib.TYPE_8UINT, band_defns)
        rsgislib.rastergis.populateStats(self.params['out_cls_75_file'], addclrtab=True, calcpyramids=True, ignorezero=False)

        band_defns = list()
        band_defns.append(rsgislib.imagecalc.BandDefn('scr', self.params['out_sum_cls_file'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_cls_85_file'], "scr>0.85?1:0", 'KEA', rsgislib.TYPE_8UINT, band_defns)
        rsgislib.rastergis.populateStats(self.params['out_cls_85_file'], addclrtab=True, calcpyramids=True, ignorezero=False)


    def required_fields(self, **kwargs):
        return ["granule", "cls_files", "out_sum_cls_file", "out_cls_50_file", "out_cls_75_file", "out_cls_85_file"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_sum_cls_file']] = 'gdal_img'
        files_dict[self.params['out_cls_50_file']] = 'gdal_img'
        files_dict[self.params['out_cls_75_file']] = 'gdal_img'
        files_dict[self.params['out_cls_85_file']] = 'gdal_img'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output files.
        if os.path.exists(self.params['out_sum_cls_file']):
            os.remove(self.params['out_sum_cls_file'])

        if os.path.exists(self.params['out_cls_50_file']):
            os.remove(self.params['out_cls_50_file'])

        if os.path.exists(self.params['out_cls_75_file']):
            os.remove(self.params['out_cls_75_file'])

        if os.path.exists(self.params['out_cls_85_file']):
            os.remove(self.params['out_cls_85_file'])

if __name__ == "__main__":
    MergeGranuleCls().std_run()


