from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import rsgislib
import logging
import os
import sys

sys.path.insert(0, "../03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class GenClearSkyCmds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        if not os.path.exists(kwargs['scn_db_file']):
            raise Exception("Sentinel-2 scene database does not exist...")

        sen2_rcd_obj = RecordSen2Process(kwargs['scn_db_file'])
        scns = sen2_rcd_obj.get_processed_scns()
        for scn in scns:
            print("\t{}".format(scn.product_id))
            if scn.ard:
                cloud_msk = self.find_first_file(scn.ard_path, "*clouds.kea", rtn_except=False)
                valid_msk = self.find_first_file(scn.ard_path, "*valid.kea", rtn_except=False)

                if (cloud_msk is not None) and (valid_msk is not None):
                    basename = self.get_file_basename(cloud_msk).replace("_clouds", "")

                    out_dir = os.path.dirname(cloud_msk)
                    out_img = os.path.join(out_dir, "{}_clearsky_refine.kea".format(basename))

                    c_dict = dict()
                    c_dict['product_id'] = scn.product_id
                    c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], scn.product_id)
                    c_dict['cloud_msk'] = cloud_msk
                    c_dict['valid_msk'] = valid_msk
                    c_dict['out_clrsky_img'] = out_img
                    if not os.path.exists(c_dict['tmp_dir']):
                        os.mkdir(c_dict['tmp_dir'])
                    self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(scn_db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("sen2_clrsky", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

    def run_check_outputs(self):
        process_tools_mod = 'calc_clear_sky_msk'
        process_tools_cls = 'ComputeSen2ClearSkyMask'
        time_sample_str = self.generate_readable_timestamp_str()
        out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
        out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
        self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)

    def run_remove_outputs(self, all_jobs=False, error_jobs=False):
        process_tools_mod = 'calc_clear_sky_msk'
        process_tools_cls = 'ComputeSen2ClearSkyMask'
        self.remove_job_outputs(process_tools_mod, process_tools_cls, all_jobs, error_jobs)


if __name__ == "__main__":
    py_script = os.path.abspath("calc_clear_sky_msk.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    create_tools = GenClearSkyCmds(cmd=script_cmd, sqlite_db_file="sen2_clrsky_cmds.db")
    create_tools.parse_cmds()
