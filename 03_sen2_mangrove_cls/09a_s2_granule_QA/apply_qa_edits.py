from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import time

logger = logging.getLogger(__name__)

class ApplyQAEdits(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='apply_qa_edits.py', descript=None)

    def do_processing(self, **kwargs):
        import rsgislib
        import rsgislib.vectorutils
        import rsgislib.segmentation
        import rsgislib.imagecalc
        import rsgislib.rastergis

        if not os.path.exists(self.params['tmp_dir']):
            os.mkdir(self.params['tmp_dir'])
            time.sleep(1)

        mng_qa_img = os.path.join(self.params['tmp_dir'], "{}_mng_qa_msk.kea".format(self.params['granule']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['mng_qa_edits_file'], self.params['mng_qa_edits_lyr'],
                                             self.params['cls_img_file'], mng_qa_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        notmng_qa_img = os.path.join(self.params['tmp_dir'], "{}_notmng_qa_msk.kea".format(self.params['granule']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['notmng_qa_edits_file'], self.params['notmng_qa_edits_lyr'],
                                             self.params['cls_img_file'], notmng_qa_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        cls_qa_apply_img = os.path.join(self.params['tmp_dir'], "{}_cls_85_qa.kea".format(self.params['granule']))
        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('cls', self.params['cls_img_file'], 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('mng', mng_qa_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('nmng', notmng_qa_img, 1))
        rsgislib.imagecalc.bandMath(cls_qa_apply_img, '(nmng==1)?0:(mng==1)?1:cls', 'KEA', rsgislib.TYPE_8UINT, bandDefns)
        rsgislib.rastergis.populateStats(cls_qa_apply_img, addclrtab=True, calcpyramids=False, ignorezero=True)

        cls_qa_clumps_img = os.path.join(self.params['tmp_dir'], "{}_cls_85_qa_clumps.kea".format(self.params['granule']))
        rsgislib.segmentation.clump(cls_qa_apply_img, cls_qa_clumps_img, 'KEA', False, 0, False)

        cls_qa_clumps_rmsml_img = os.path.join(self.params['tmp_dir'], "{}_cls_85_qa_clumps_rmsml.kea".format(self.params['granule']))
        rsgislib.segmentation.rmSmallClumps(cls_qa_clumps_img, cls_qa_clumps_rmsml_img, 3, 'KEA')
        rsgislib.rastergis.populateStats(cls_qa_clumps_rmsml_img, addclrtab=True, calcpyramids=False, ignorezero=True)

        rsgislib.imagecalc.imageMath(cls_qa_clumps_rmsml_img, self.params['cls_out_file'], 'b1>0>1:0', 'KEA', rsgislib.TYPE_8UINT)
        rsgislib.rastergis.populateStats(self.params['cls_out_file'], addclrtab=True, calcpyramids=True, ignorezero=True)

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])


    def required_fields(self, **kwargs):
        return ["granule", "mng_qa_edits_file", "mng_qa_edits_lyr", "notmng_qa_edits_file",
                "notmng_qa_edits_lyr", "cls_img_file", "cls_out_file", "tmp_dir"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['cls_out_file']] = 'gdal_image'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output files.
        if os.path.exists(self.params['cls_out_file']):
            os.remove(self.params['cls_out_file'])

        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

if __name__ == "__main__":
    ApplyQAEdits().std_run()


