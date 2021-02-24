from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import logging
import os
import sys
import rsgislib
import glob

logger = logging.getLogger(__name__)

class GenMergeGranuleClsCmds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        rsgis_utils = rsgislib.RSGISPyUtils()
        granule_lst = rsgis_utils.readTextFile2List(kwargs['granule_lst'])

        for granule in granule_lst:
            print(granule)
            cls_files = glob.glob(os.path.join(kwargs['cls_scn_dir'], "*{}*sum_cls.kea".format(granule)))
            if len(cls_files) > 0:
                print("\t n scene classes: {}".format(len(cls_files)))
                c_dict = dict()
                c_dict['granule'] = granule
                c_dict['cls_files'] = cls_files
                c_dict['out_sum_cls_file'] = os.path.join(kwargs['out_granule_dir'], "{}_sum_cls.kea".format(granule))
                c_dict['out_cls_50_file'] = os.path.join(kwargs['out_granule_dir'], "{}_cls_50.kea".format(granule))
                c_dict['out_cls_75_file'] = os.path.join(kwargs['out_granule_dir'], "{}_cls_75.kea".format(granule))
                c_dict['out_cls_85_file'] = os.path.join(kwargs['out_granule_dir'], "{}_cls_85.kea".format(granule))
                if not (os.path.exists(c_dict['out_sum_cls_file']) and os.path.exists(c_dict['out_cls_50_file']) and os.path.exists(c_dict['out_cls_75_file']) and os.path.exists(c_dict['out_cls_85_file'])):
                    self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(granule_lst='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/sen2_roi_granule_lst.txt',
                              cls_scn_dir='/scratch/a.pfb/gmw_v2_gapfill/data/sum_scn_cls_files',
                              out_granule_dir='/scratch/a.pfb/gmw_v2_gapfill/data/sum_scn_granule_files')
        self.pop_params_db()
        self.create_slurm_sub_sh("merge_granule_cls", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("merge_granule_cls.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'merge_granule_cls'
    process_tools_cls = 'MergeGranuleCls'

    create_tools = GenMergeGranuleClsCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                      lock_file_path="./gmw_gapfill_lock_file.txt",
                                      process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
