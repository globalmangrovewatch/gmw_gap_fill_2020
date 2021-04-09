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

            gmw_add_file = os.path.join(kwargs['gmw_diff_dir'], '{}_gmw_add_v2_v3.kea'.format(tile_basename))
            gmw_rmv_file = os.path.join(kwargs['gmw_diff_dir'], '{}_gmw_rmv_v2_v3.kea'.format(tile_basename))

            out_add_file = os.path.join(kwargs['out_dir'], '{}_gmw_add_v2_v3.gpkg'.format(tile_basename))
            out_rmv_file = os.path.join(kwargs['out_dir'], '{}_gmw_rmv_v2_v3.gpkg'.format(tile_basename))

            out_add_cmp_file = os.path.join(kwargs['out_dir'], "{}_gmw_add_v2_v3.txt".format(tile_basename))
            out_rmv_cmp_file = os.path.join(kwargs['out_dir'], "{}_gmw_rmv_v2_v3.txt".format(tile_basename))

            if (not os.path.exists(out_add_cmp_file)):
                c_dict = dict()
                c_dict['tile_img'] = tile
                c_dict['gmw_v3_img'] = gmw_add_file
                c_dict['out_file'] = out_add_file
                c_dict['out_cmp_file'] = out_add_cmp_file
                self.params.append(c_dict)

            if (not os.path.exists(out_rmv_cmp_file)):
                c_dict = dict()
                c_dict['tile_img'] = tile
                c_dict['gmw_v3_img'] = gmw_rmv_file
                c_dict['out_file'] = out_rmv_file
                c_dict['out_cmp_file'] = out_rmv_cmp_file
                self.params.append(c_dict)


    def run_gen_commands(self):
        self.gen_command_info(ref_tiles_path='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_ref_tiles/*.kea',
                              gmw_diff_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_diff_v2_v3_qa',
                              out_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_diff_v2_v3_qa_vec')
        self.pop_params_db()
        self.create_slurm_sub_sh("vectorise_gmw_lyr", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("vectorise_gmw_lyr.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'vectorise_gmw_lyr'
    process_tools_cls = 'VectoriseGMWLyr'

    create_tools = GenCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                         lock_file_path="./gmw_gapfill_lock_file.txt",
                                         process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
