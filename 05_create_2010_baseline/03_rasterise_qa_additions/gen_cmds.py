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

            out_file = os.path.join(kwargs['out_dir'], '{}_QAXtrs.kea'.format(tile_basename))
            if not os.path.exists(out_file):
                c_dict = dict()
                c_dict['tile_img'] = tile
                c_dict['gapfill_qa_vec'] = kwargs['gapfill_qa_vec']
                c_dict['gapfill_qa_lyr'] = kwargs['gapfill_qa_lyr']
                c_dict['french_qa_vec'] = kwargs['french_qa_vec']
                c_dict['french_qa_lyr'] = kwargs['french_qa_lyr']
                c_dict['out_file'] = out_file
                c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], "{}_qa_edits".format(tile_basename))
                if not os.path.exists(c_dict['tmp_dir']):
                    os.mkdir(c_dict['tmp_dir'])
                self.params.append(c_dict)


    def run_gen_commands(self):
        self.gen_command_info(ref_tiles_path='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_ref_tiles/*.kea',
                              gapfill_qa_vec='/scratch/a.pfb/gmw_v2_gapfill/scripts/03_sen2_mangrove_cls/09a_s2_granule_QA/mangroves_qa.gpkg',
                              gapfill_qa_lyr='mangroves_qa',
                              french_qa_vec='/scratch/a.pfb/gmw_v2_gapfill/data/IUCNFrenchTerritories_mangroves_sngl.gpkg',
                              french_qa_lyr='mangroves',
                              out_dir='/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_qa_xtrs_base',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("rasterise_qa_xtrs", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("rasterise_qa_xtrs.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'rasterise_qa_xtrs'
    process_tools_cls = 'RasteriseQAExtras'

    create_tools = GenCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                         lock_file_path="./gmw_gapfill_lock_file.txt",
                                         process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
