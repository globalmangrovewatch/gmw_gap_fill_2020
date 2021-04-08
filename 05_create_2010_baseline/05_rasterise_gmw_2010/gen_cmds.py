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

            out_file = os.path.join(kwargs['out_dir'], '{}_GMW2010_v2.kea'.format(tile_basename))
            if not os.path.exists(out_file):
                c_dict = dict()
                c_dict['tile_img'] = tile
                c_dict['gmw_vec'] = kwargs['gmw_vec']
                c_dict['gmw2010_lyr'] = kwargs['gmw2010_lyr']
                c_dict['out_file'] = out_file
                self.params.append(c_dict)


    def run_gen_commands(self):
        self.gen_command_info(ref_tiles_path='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_ref_tiles/*.kea',
                              gmw_vec='/scratch/a.pfb/gmw_v2_gapfill/data/GMW_MangroveExtent_WGS84_v2.0.gpkg',
                              gmw2010_lyr='gmw2010v2.0',
                              out_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_2010_base')
        self.pop_params_db()
        self.create_slurm_sub_sh("rasterise_gapfill", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("rasterise_gmw2010.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'rasterise_gmw2010'
    process_tools_cls = 'RasteriseGMW2010'

    create_tools = GenCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                         lock_file_path="./gmw_gapfill_lock_file.txt",
                                         process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()