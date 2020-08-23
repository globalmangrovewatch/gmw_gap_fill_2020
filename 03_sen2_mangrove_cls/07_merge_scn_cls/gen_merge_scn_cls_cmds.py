from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import logging
import os
import sys
import random
import rsgislib
import glob

sys.path.insert(0, "../../01_sen2_ard/03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class GenMergeScnClsCmds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        if not os.path.exists(kwargs['scn_db_file']):
            raise Exception("Sentinel-2 scene database does not exist...")

        rsgis_utils = rsgislib.RSGISPyUtils()
        granule_lst = rsgis_utils.readTextFile2List(kwargs['granule_lst'])

        sen2_rcd_obj = RecordSen2Process(kwargs['scn_db_file'])

        for granule in granule_lst:
            print(granule)
            scns = sen2_rcd_obj.get_processed_scns(granule)
            for scn in scns:
                print(scn.product_id)
                cls_scn_dir = os.path.join(kwargs['cls_scn_dir'], scn.product_id)
                if os.path.exists(cls_scn_dir):
                    cls_files = glob.glob(os.path.join(cls_scn_dir, "*.kea"))
                    if len(cls_files) > 0:
                        clrsky_img = self.find_first_file(scn.ard_path, "*clearsky_refine.kea", rtn_except=False)
                        c_dict = dict()
                        c_dict['scn_id'] = scn.product_id
                        c_dict['cls_files'] = cls_files
                        c_dict['clr_sky']  = clrsky_img
                        c_dict['out_sum_cls_file'] = os.path.join(kwargs['out_scn_dir'], "{}_sum_cls.kea".format(scn.product_id))
                        c_dict['out_cls_25_file'] = os.path.join(kwargs['out_scn_dir'], "{}_cls_25.kea".format(scn.product_id))
                        c_dict['out_cls_50_file'] = os.path.join(kwargs['out_scn_dir'], "{}_cls_50.kea".format(scn.product_id))
                        c_dict['out_cls_75_file'] = os.path.join(kwargs['out_scn_dir'], "{}_cls_75.kea".format(scn.product_id))
                        self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(scn_db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db',
                              cls_scn_dir='/scratch/a.pfb/gmw_v2_gapfill/data/scn_cls_files',
                              out_scn_dir='/scratch/a.pfb/gmw_v2_gapfill/data/sum_scn_cls_files')
        self.pop_params_db()
        self.create_slurm_sub_sh("merge_scn_cls", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("merge_scn_cls.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'merge_scn_cls'
    process_tools_cls = 'MergeScnCls'

    create_tools = GenMergeScnClsCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                      lock_file_path="./gmw_gapfill_lock_file.txt",
                                      process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
