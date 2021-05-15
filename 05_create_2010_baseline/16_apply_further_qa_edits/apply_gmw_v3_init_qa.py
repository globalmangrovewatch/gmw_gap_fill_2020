from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib
import rsgislib.imagecalc
import rsgislib.rastergis

logger = logging.getLogger(__name__)


class ApplyGMWQAEdits(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='apply_gmw_v3_init_qa.py', descript=None)

    def do_processing(self, **kwargs):
        if not os.path.exists(self.params['tmp_dir']):
            os.mkdir(self.params['tmp_dir'])

        rm_pxls_img = os.path.join(self.params['tmp_dir'], 'rm_pxls.kea')
        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('rmadd', self.params['rmadd_img'], 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('added', self.params['gmw_add_diff_img'], 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('rmvmng', self.params['rmvmng_img'], 1))
        rsgislib.imagecalc.bandMath(rm_pxls_img, '(rmvmng==1)?1:(rmadd==1)&&(added==1)?1:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)
        rsgislib.rastergis.populateStats(rm_pxls_img, addclrtab=True, calcpyramids=True, ignorezero=True)

        add_pxls_img = os.path.join(self.params['tmp_dir'], 'add_pxls.kea')
        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('restore', self.params['restore_img'], 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('removed', self.params['gmw_rmv_diff_img'], 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('addmng', self.params['addmng_img'], 1))
        rsgislib.imagecalc.bandMath(add_pxls_img, '(addmng==1)?1:(restore==1)&&(removed==1)?1:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)
        rsgislib.rastergis.populateStats(add_pxls_img, addclrtab=True, calcpyramids=True, ignorezero=True)


        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('rmv', rm_pxls_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('add', add_pxls_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('gmw', self.params['gmw_v3_img'], 1))
        rsgislib.imagecalc.bandMath(self.params['out_file'], '(rmv==1)?0:(add==1)?1:gmw', 'KEA', rsgislib.TYPE_8UINT, bandDefns)
        rsgislib.rastergis.populateStats(self.params['out_file'], addclrtab=True, calcpyramids=True, ignorezero=True)

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])


    def required_fields(self, **kwargs):
        return ["tile_img", "gmw_v3_img", "gmw_add_diff_img", "gmw_rmv_diff_img", "addmng_img", "rmvmng_img", "restore_img", "rmadd_img", "out_file"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_file']] = 'gdal_image'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output file.
        if os.path.exists(self.params['out_file']):
            os.remove(self.params['out_file'])

        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

if __name__ == "__main__":
    ApplyGMWQAEdits().std_run()


