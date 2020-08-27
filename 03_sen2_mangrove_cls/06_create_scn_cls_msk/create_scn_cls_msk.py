from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import time
import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc
import rsgislib.imageutils

logger = logging.getLogger(__name__)


class CreateScnClsMsk(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='create_scn_cls_msk.py', descript=None)

    def do_processing(self, **kwargs):
        if not os.path.exists(self.params['tmp_dir']):
            os.mkdir(self.params['tmp_dir'])
            time.sleep(1)

        veg_msk_img = os.path.join(self.params['tmp_dir'], "{}_veg_msk.kea".format(self.params['scn_id']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['gmw_veg_msk_vec'], self.params['gmw_veg_msk_lyr'],
                                             self.params['vld_img'], veg_msk_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        hab_msk_img = os.path.join(self.params['tmp_dir'], "{}_hab_msk.kea".format(self.params['scn_id']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['gmw_hab_msk_vec'], self.params['gmw_hab_msk_lyr'],
                                             self.params['vld_img'], hab_msk_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        hab_add_msk_img = os.path.join(self.params['tmp_dir'], "{}_hab_add_msk.kea".format(self.params['scn_id']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['gmw_hab_msk_mng_add_vec'], self.params['gmw_hab_msk_mng_add_lyr'],
                                             self.params['vld_img'], hab_add_msk_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        hab_rm_msk_img = os.path.join(self.params['tmp_dir'], "{}_hab_rm_msk.kea".format(self.params['scn_id']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['gmw_hab_msk_mng_rm_vec'], self.params['gmw_hab_msk_mng_rm_lyr'],
                                             self.params['vld_img'], hab_rm_msk_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        hab_msk_up_img = os.path.join(self.params['tmp_dir'], "{}_hab_msk_upd.kea".format(self.params['scn_id']))
        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('hab', hab_msk_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('hab_add', hab_add_msk_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('hab_rm', hab_rm_msk_img, 1))
        rsgislib.imagecalc.bandMath(hab_msk_up_img, '(hab_add==1)?1:(hab_rm==1)?0:hab', 'KEA', rsgislib.TYPE_8UINT, bandDefns)

        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('hab', hab_msk_up_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('clrsky', self.params['clrsky_img'], 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('veg', veg_msk_img, 1))
        rsgislib.imagecalc.bandMath(self.params['out_cls_msk_file'], '(hab==1) && (clrsky==1) && (veg==1)?1:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)

        n_smpls = rsgislib.imagecalc.countPxlsOfVal(self.params['out_cls_msk_file'], vals=[1])
        print(n_smpls)

        if n_smpls[0] == 0:
            os.remove(self.params['out_cls_msk_file'])

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["scn_id", "vld_img", "clrsky_img", "gmw_veg_msk_vec", "gmw_veg_msk_lyr",
                "gmw_hab_msk_vec", "gmw_hab_msk_lyr", "gmw_hab_msk_mng_add_vec", "gmw_hab_msk_mng_add_lyr",
                "gmw_hab_msk_mng_rm_vec", "gmw_hab_msk_mng_rm_lyr", "out_cls_msk_file", "tmp_dir"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_cls_msk_file']] = 'gdalimg'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

        # Remove the output file.
        if os.path.exists(self.params['out_cls_msk_file']):
            os.remove(self.params['out_cls_msk_file'])

if __name__ == "__main__":
    CreateScnClsMsk().std_run()


