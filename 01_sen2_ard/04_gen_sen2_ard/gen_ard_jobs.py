from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import logging
import os
from ..03_find_dwnld_scns.sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class FindSen2ScnsGenDwnlds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        if not os.path.exists(kwargs['scn_db_file']):
            raise Exception("Sentinel-2 scene database does not exist...")
        sen2_rcd_obj = RecordSen2Process(kwargs['scn_db_file'])
        scns = sen2_rcd_obj.get_scns_ard()
        for scn in scns:
            c_dict = dict()
            c_dict['product_id'] = scn.product_id
            c_dict['dwnld_path'] = scn.download_path
            c_dict['ard_path'] = os.path.join(kwargs['ard_path'], scn.product_id)
            c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], scn.product_id)
            c_dict['scn_db_file'] = kwargs['scn_db_file']
            c_dict['dem'] = kwargs['dem']
            if not os.path.exists(c_dict['ard_path']):
                os.mkdir(c_dict['ard_path'])
            if not os.path.exists(c_dict['tmp_dir']):
                os.mkdir(c_dict['tmp_dir'])
            self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(scn_db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db',
                              ard_path='/scratch/a.pfb/gmw_v2_gapfill/data/ard',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp',
                              dem='/scratch/a.pfb/srtm_global_mosaic_1arc_v3.kea')
        self.pop_params_db()
        self.create_slurm_sub_sh("ard_sen2_scns", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

    def run_check_outputs(self):
        process_tools_mod = 'perform_ard_process'
        process_tools_cls = 'PerformScnARD'
        time_sample_str = self.generate_readable_timestamp_str()
        out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
        out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
        self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)


if __name__ == "__main__":
    py_script = os.path.abspath("perform_ard_process.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    create_tools = FindSen2ScnsGenDwnlds(cmd=script_cmd, sqlite_db_file="ard_sen2_scns.db")
    create_tools.parse_cmds()