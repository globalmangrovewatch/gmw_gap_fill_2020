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

            gmw_v2_img = os.path.join(kwargs['gmw_v2_dir'], '{}_GMW2010_v2.kea'.format(tile_basename))
            gapfill_img = os.path.join(kwargs['gapfill_dir'], '{}_gmw_gapfill_fnl.kea'.format(tile_basename))
            s2vmsk_img = os.path.join(kwargs['s2vldmsk_dir'], '{}_S2VldMsk.kea'.format(tile_basename))

            out_file = os.path.join(kwargs['out_dir'], '{}_gmw_v3_init.kea'.format(tile_basename))

            if not os.path.exists(out_file):
                c_dict = dict()
                c_dict['tile_img'] = tile
                c_dict['gmw_v2_img'] = gmw_v2_img
                c_dict['gapfill_img'] = gapfill_img
                c_dict['s2vmsk_img'] = s2vmsk_img
                c_dict['out_file'] = out_file
                self.params.append(c_dict)


    def run_gen_commands(self):
        self.gen_command_info(ref_tiles_path='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_ref_tiles/*.kea',
                              gapfill_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_gapfill_edits',
                              gmw_v2_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_2010_base',
                              s2vldmsk_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_gapfill_vmsk_base',
                              out_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_init_v3')
        self.pop_params_db()
        self.create_slurm_sub_sh("merge_gmw_edits_v3", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("merge_gmw_edits.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'merge_gmw_edits'
    process_tools_cls = 'MergeGMWEdits'

    create_tools = GenCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                         lock_file_path="./gmw_gapfill_lock_file.txt",
                                         process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
