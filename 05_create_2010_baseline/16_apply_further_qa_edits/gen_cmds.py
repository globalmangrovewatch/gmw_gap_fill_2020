from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import logging
import os
import glob
import pathlib

logger = logging.getLogger(__name__)

class GenCmds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        tiles = glob.glob(kwargs['ref_tiles_path'])
        for tile in tiles:
            tile_basename = self.get_file_basename(tile)

            gmw_v3_img = os.path.join(kwargs['gmw_v3_dir'], '{}_gmw_v3_init_qad.kea'.format(tile_basename))

            gmw_add_diff_img = os.path.join(kwargs['gmw_v3_diff_dir'], '{}_gmw_add_v2_v3.kea'.format(tile_basename))
            gmw_rmv_diff_img = os.path.join(kwargs['gmw_v3_diff_dir'], '{}_gmw_rmv_v2_v3.kea'.format(tile_basename))

            addmng_img = os.path.join(kwargs['qa_edits_dir'], '{}_addman_qa.kea'.format(tile_basename))
            rmvmng_img = os.path.join(kwargs['qa_edits_dir'], '{}_rmvmng_qa.kea'.format(tile_basename))
            restore_img = os.path.join(kwargs['qa_edits_dir'], '{}_restore_qa.kea'.format(tile_basename))
            rmadd_img = os.path.join(kwargs['qa_edits_dir'], '{}_rmadd_qa.kea'.format(tile_basename))

            out_file = os.path.join(kwargs['out_dir'], '{}_gmw_v3_init_qad.kea'.format(tile_basename))

            if not os.path.exists(out_file):
                print('rm {}'.format(rmadd_img))
                c_dict = dict()
                c_dict['tile_img'] = tile
                c_dict['gmw_v3_img'] = gmw_v3_img
                c_dict['gmw_add_diff_img'] = gmw_add_diff_img
                c_dict['gmw_rmv_diff_img'] = gmw_rmv_diff_img
                c_dict['addmng_img'] = addmng_img
                c_dict['rmvmng_img'] = rmvmng_img
                c_dict['restore_img'] = restore_img
                c_dict['rmadd_img'] = rmadd_img
                c_dict['out_file'] = out_file
                c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], "{}_qa_v3_init_edits".format(tile_basename))
                if not os.path.exists(c_dict['tmp_dir']):
                    os.mkdir(c_dict['tmp_dir'])
                self.params.append(c_dict)


    def run_gen_commands(self):
        self.gen_command_info(ref_tiles_path='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_ref_tiles/*.kea',
                              qa_edits_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_v3_further_qa_edits',
                              gmw_v3_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_init_v3_qa',
                              gmw_v3_diff_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_diff_v2_v3_qa',
                              out_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_init_v3_further_qa',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("gmw_init_v3_further_qa", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("apply_gmw_v3_init_qa.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'apply_gmw_v3_init_qa'
    process_tools_cls = 'ApplyGMWQAEdits'

    create_tools = GenCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                         lock_file_path="./gmw_gapfill_lock_file.txt",
                                         process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
