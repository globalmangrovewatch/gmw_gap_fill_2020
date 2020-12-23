from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import logging
import os
import sys
import random

sys.path.insert(0, "../../01_sen2_ard/03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class GenExtractSamplesCmds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        if not os.path.exists(kwargs['scn_db_file']):
            raise Exception("Sentinel-2 scene database does not exist...")

        sen2_rcd_obj = RecordSen2Process(kwargs['scn_db_file'])
        scns = sen2_rcd_obj.get_processed_scns()
        err_scns = []
        for scn in scns:
            #print(scn.product_id)

            vld_img = self.find_first_file(scn.ard_path, "*valid.kea", rtn_except=False)
            clrsky_img = self.find_first_file(scn.ard_path, "*clearsky_refine.kea", rtn_except=False)
            sref_img = self.find_first_file(scn.ard_path, "*vmsk_rad_srefdem_stdsref.kea", rtn_except=False)
            if (vld_img is None) or (clrsky_img is None) or (sref_img is None):
                clouds_img = self.find_first_file(scn.ard_path, "*clouds.kea", rtn_except=False)
                if clouds_img is None:
                    print(scn.product_id)
                    print("\t***ERROR***: {}".format(scn.ard_path))
                    err_scns.append(scn.ard_path)
            else:
                out_cls_msk_file = os.path.join(kwargs['out_cls_msk_dir'], "{}_cls_msk.kea".format(scn.product_id))
                if not os.path.exists(out_cls_msk_file):
                    c_dict = dict()
                    c_dict['scn_id'] = scn.product_id
                    c_dict['vld_img'] = vld_img
                    c_dict['clrsky_img'] = clrsky_img
                    c_dict['gmw_veg_msk_vec'] = kwargs['gmw_veg_msk_vec']
                    c_dict['gmw_veg_msk_lyr'] = kwargs['gmw_veg_msk_lyr']
                    c_dict['gmw_hab_msk_vec'] = kwargs['gmw_hab_msk_vec']
                    c_dict['gmw_hab_msk_lyr'] = kwargs['gmw_hab_msk_lyr']
                    c_dict['gmw_hab_msk_mng_add_vec'] = kwargs['gmw_hab_msk_mng_add_vec']
                    c_dict['gmw_hab_msk_mng_add_lyr'] = kwargs['gmw_hab_msk_mng_add_lyr']
                    c_dict['gmw_hab_msk_mng_rm_vec'] = kwargs['gmw_hab_msk_mng_rm_vec']
                    c_dict['gmw_hab_msk_mng_rm_lyr'] = kwargs['gmw_hab_msk_mng_rm_lyr']
                    c_dict['out_cls_msk_file'] = out_cls_msk_file
                    c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], "{}_cls_msk".format(scn.product_id))
                    if not os.path.exists(c_dict['tmp_dir']):
                        os.mkdir(c_dict['tmp_dir'])
                    self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(scn_db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db',
                              gmw_veg_msk_vec='/scratch/a.pfb/gmw_v2_gapfill/data/granule_veg_msks.gpkg',
                              gmw_veg_msk_lyr='granule_veg_msks',
                              gmw_hab_msk_vec='/scratch/a.pfb/gmw_v2_gapfill/data/GMW_Mangrove_Habitat_v4.gpkg',
                              gmw_hab_msk_lyr='gmw_hab_v4',
                              gmw_hab_msk_mng_add_vec='/scratch/a.pfb/gmw_v2_gapfill/data/habitat_additions_v3_to_v4.gpkg',
                              gmw_hab_msk_mng_add_lyr='habitat_additions_v3_to_v4',
                              gmw_hab_msk_mng_rm_vec='/scratch/a.pfb/gmw_v2_gapfill/data/habitat_remove_v3_to_v4.gpkg',
                              gmw_hab_msk_mng_rm_lyr='habitat_remove_v3_to_v4',
                              out_cls_msk_dir='/scratch/a.pfb/gmw_v2_gapfill/data/scn_cls_msk_imgs',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("create_scn_cls_msk", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("create_scn_cls_msk.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'create_scn_cls_msk'
    process_tools_cls = 'CreateScnClsMsk'

    create_tools = GenExtractSamplesCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                         lock_file_path="./gmw_gapfill_lock_file.txt",
                                         process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
