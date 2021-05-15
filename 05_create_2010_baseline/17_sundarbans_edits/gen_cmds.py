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

            out_file = os.path.join(kwargs['out_dir'], '{}_gmw_v3_init_qad.kea'.format(tile_basename))

            if not os.path.exists(out_file):
                c_dict = dict()
                c_dict['tile_basename'] = tile_basename
                c_dict['tile_img'] = tile
                c_dict['gmw_v3_img'] = gmw_v3_img
                c_dict['sundarbans_vec_file'] = kwargs['sundarbans_vec_file']
                c_dict['sundarbans_vec_lyr'] = kwargs['sundarbans_vec_lyr']
                c_dict['sundarbans_roi_vec_file'] = kwargs['sundarbans_roi_vec_file']
                c_dict['sundarbans_roi_vec_lyr'] = kwargs['sundarbans_roi_vec_lyr']
                c_dict['out_file'] = out_file
                c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], "{}_sundarbans_edits".format(tile_basename))
                if not os.path.exists(c_dict['tmp_dir']):
                    os.mkdir(c_dict['tmp_dir'])
                self.params.append(c_dict)


    def run_gen_commands(self):
        self.gen_command_info(ref_tiles_path='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_ref_tiles/*.kea',
                              qa_edits_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_init_v3_further_qa',
                              sundarbans_vec_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/05_create_2010_baseline/17_sundarbans_edits/sundarbans_2010_wgs84.gpkg',
                              sundarbans_vec_lyr='sundarbans_2010_wgs84',
                              sundarbans_roi_vec_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/05_create_2010_baseline/17_sundarbans_edits/edits_roi.gpkg',
                              sundarbans_roi_vec_lyr='edits_roi',
                              out_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_init_v3_further_qa_part2',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("gmw_init_v3_further_qa_part2", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("apply_sundarbans_edits.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'apply_sundarbans_edits'
    process_tools_cls = 'ApplySundarbansEdits'

    create_tools = GenCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                         lock_file_path="./gmw_gapfill_lock_file.txt",
                                         process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
