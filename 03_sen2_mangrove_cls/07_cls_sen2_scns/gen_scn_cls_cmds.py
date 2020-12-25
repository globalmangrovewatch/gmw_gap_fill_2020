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
                    print("***ERROR***: {}".format(scn.ard_path))
                    err_scns.append(scn.ard_path)
            else:
                out_scn_dir = os.path.join(kwargs['out_cls_scn_dir'], scn.product_id)
                if not os.path.exists(out_scn_dir):
                    os.mkdir(out_scn_dir)

                cls_msk_file = os.path.join(kwargs['cls_msks_dir'], "{}_cls_msk.kea".format(scn.product_id))
                scn_mng_smps_file = os.path.join(kwargs['cls_scn_smps_dir'], "{}_mng_smpls.h5".format(scn.product_id))
                scn_oth_smps_file = os.path.join(kwargs['cls_scn_smps_dir'], "{}_oth_smpls.h5".format(scn.product_id))
                #print(cls_msk_file)
                if os.path.exists(cls_msk_file):
                    for i in range(kwargs['n_sample_sets']):
                        cls_mdl_file = os.path.join(kwargs['cls_files_dir'], 'sen2_gfill_opt_xgb_cls_trained_{}.mdl'.format(i + 1))
                        cls_params_file = os.path.join(kwargs['cls_files_dir'], 'sen2_gfill_opt_xgb_cls_{}.json'.format(i + 1))
                        out_cls_file = os.path.join(out_scn_dir, "sen2_cls_{}.kea".format(i + 1))
                        if not os.path.exists(out_cls_file):
                            print("{}".format(out_cls_file))
                            c_dict = dict()
                            c_dict['scn_id'] = scn.product_id
                            c_dict['vld_img'] = vld_img
                            c_dict['clrsky_img'] = clrsky_img
                            c_dict['sref_img'] = sref_img
                            c_dict['cls_msk_img'] = cls_msk_file
                            c_dict['cls_mdl_file'] = cls_mdl_file
                            c_dict['cls_params_file'] = cls_params_file
                            c_dict['scn_mng_smps_file'] = scn_mng_smps_file
                            c_dict['scn_oth_smps_file'] = scn_oth_smps_file
                            c_dict['out_cls_file'] = out_cls_file
                            c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], "{}_apl_cls_{}".format(scn.product_id, i + 1))
                            if not os.path.exists(c_dict['tmp_dir']):
                                os.mkdir(c_dict['tmp_dir'])
                            self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(scn_db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db',
                              samples_dir='/scratch/a.pfb/gmw_v2_gapfill/data/set_samples_h5',
                              n_sample_sets=100,
                              cls_msks_dir='/scratch/a.pfb/gmw_v2_gapfill/data/scn_cls_msk_imgs',
                              cls_files_dir='/scratch/a.pfb/gmw_v2_gapfill/data/opt_cls_files',
                              cls_scn_smps_dir='/scratch/a.pfb/gmw_v2_gapfill/data/scn_h5_samples',
                              out_cls_scn_dir='/scratch/a.pfb/gmw_v2_gapfill/data/scn_cls_files',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("apply_xgb_cls", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("apply_scn_cls.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'apply_scn_cls'
    process_tools_cls = 'ApplyXGBClass'

    create_tools = GenExtractSamplesCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                         lock_file_path="./gmw_gapfill_lock_file.txt",
                                         process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
