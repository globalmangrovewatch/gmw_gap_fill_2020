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
            cls_img_85_file = os.path.join(kwargs['granule_cls_dir'], "{}_cls_85.kea".format(granule))

            cls_qa_img_85_file = os.path.join(kwargs['out_vec_cls_dir'], "{}_cls_85_qa.kea".format(granule))

            if os.path.exists(cls_img_85_file) and (not os.path.exists(cls_qa_img_85_file)):
                c_dict = dict()
                c_dict['granule'] = granule
                c_dict['mng_qa_edits_file'] = kwargs['mng_qa_edits_file']
                c_dict['mng_qa_edits_lyr'] = kwargs['mng_qa_edits_lyr']
                c_dict['notmng_qa_edits_file'] = kwargs['notmng_qa_edits_file']
                c_dict['notmng_qa_edits_lyr'] = kwargs['notmng_qa_edits_lyr']
                c_dict['cls_img_file'] = cls_img_85_file
                c_dict['cls_out_file'] = cls_qa_img_85_file
                c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], "{}_qa_edits".format(granule))
                if not os.path.exists(c_dict['tmp_dir']):
                    os.mkdir(c_dict['tmp_dir'])
                self.params.append(c_dict)


    def run_gen_commands(self):
        self.gen_command_info(granule_lst='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/sen2_roi_granule_lst.txt',
                              granule_cls_dir='/scratch/a.pfb/gmw_v2_gapfill/data/sum_scn_granule_files',
                              mng_qa_edits_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/03_sen2_mangrove_cls/09a_s2_granule_QA/mangroves_qa.gpkg',
                              mng_qa_edits_lyr='mangroves_qa',
                              notmng_qa_edits_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/03_sen2_mangrove_cls/09a_s2_granule_QA/not_mangroves_qa.gpkg',
                              notmng_qa_edits_lyr='not_mangroves_qa',
                              out_vec_cls_dir='/scratch/a.pfb/gmw_v2_gapfill/data/sum_scn_granule_qa_files',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("qa_granule_cls", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("apply_qa_edits.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'apply_qa_edits'
    process_tools_cls = 'ApplyQAEdits'

    create_tools = GenMergeGranuleClsCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                      lock_file_path="./gmw_gapfill_lock_file.txt",
                                      process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
