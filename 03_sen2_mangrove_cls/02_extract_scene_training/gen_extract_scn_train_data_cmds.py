from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import rsgislib
import logging
import os
import sys

sys.path.insert(0, "../../01_sen2_ard/03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class GenExtractSamplesCmds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        if not os.path.exists(kwargs['scn_db_file']):
            raise Exception("Sentinel-2 scene database does not exist...")

        sen2_rcd_obj = RecordSen2Process(kwargs['scn_db_file'])
        scns = sen2_rcd_obj.get_processed_scns()
        err_scns = []
        for scn in scns:
            print(scn.product_id)
            scn_out_mng_file = os.path.join(kwargs['granule_out_h5_samples_path'], "{}_mng_smpls.h5".format(scn.product_id))
            scn_out_oth_file = os.path.join(kwargs['granule_out_h5_samples_path'], "{}_oth_smpls.h5".format(scn.product_id))
            if (not os.path.exists(scn_out_mng_file)) or (not os.path.exists(scn_out_oth_file)):
                vld_img = self.find_first_file(scn.ard_path, "*valid.kea", rtn_except=False)
                clrsky_img = self.find_first_file(scn.ard_path, "*clearsky_refine.kea", rtn_except=False)
                sref_img = self.find_first_file(scn.ard_path, "*vmsk_rad_srefdem_stdsref.kea", rtn_except=False)
                if (vld_img is None) or (clrsky_img is None) or (sref_img is None):
                    clouds_img = self.find_first_file(scn.ard_path, "*clouds.kea", rtn_except=False)
                    if clouds_img is None:
                        print("***ERROR***: {}".format(scn.ard_path))
                        err_scns.append(scn.ard_path)
                else:
                    c_dict = dict()
                    c_dict['scn_id'] = scn.product_id
                    c_dict['vld_img'] = vld_img
                    c_dict['clrsky_img'] = clrsky_img
                    c_dict['sref_img'] = sref_img
                    c_dict['samples_vec_file'] = kwargs['samples_vec_file']
                    c_dict['samples_vec_lyr'] = kwargs['samples_vec_lyr']
                    c_dict['scn_out_mng_file'] = scn_out_mng_file
                    c_dict['scn_out_oth_file'] = scn_out_oth_file
                    c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], "{}_extract_smpls".format(scn.product_id))
                    if not os.path.exists(c_dict['tmp_dir']):
                        os.mkdir(c_dict['tmp_dir'])
                    self.params.append(c_dict)
        print("ERRORS:")
        for err_scn in err_scns:
            print(err_scn)

    def run_gen_commands(self):
        self.gen_command_info(scn_db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db',
                              samples_vec_file='/scratch/a.pfb/gmw_v2_gapfill/data/granule_mang_train_smpls_uid.gpkg',
                              samples_vec_lyr='samples',
                              granule_out_h5_samples_path='/scratch/a.pfb/gmw_v2_gapfill/data/scn_h5_samples',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("extract_sen2_samples", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

    def run_check_outputs(self):
        process_tools_mod = 'extract_scn_train_data'
        process_tools_cls = 'ExtractSceneTrainSamples'
        time_sample_str = self.generate_readable_timestamp_str()
        out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
        out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
        self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)

    def run_remove_outputs(self, all_jobs=False, error_jobs=False):
        process_tools_mod = 'extract_scn_train_data'
        process_tools_cls = 'ExtractSceneTrainSamples'
        self.remove_job_outputs(process_tools_mod, process_tools_cls, all_jobs, error_jobs)


if __name__ == "__main__":
    py_script = os.path.abspath("create_granule_veg_msk.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    create_tools = GenExtractSamplesCmds(cmd=script_cmd, sqlite_db_file="sen2_granule_veg_msks.db")
    create_tools.parse_cmds()
