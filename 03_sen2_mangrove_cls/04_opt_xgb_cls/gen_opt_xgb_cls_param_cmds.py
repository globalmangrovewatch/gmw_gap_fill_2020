from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import logging
import os

logger = logging.getLogger(__name__)

class GenExtractSamplesCmds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        for i in range(kwargs['n_sample_sets']):
            mng_train_smps_file = os.path.join(kwargs['samples_dir'], "mng_train_samples_{}_opt.h5".format(i + 1))
            mng_valid_smps_file = os.path.join(kwargs['samples_dir'], "mng_valid_samples_{}.h5".format(i + 1))

            oth_train_smps_file = os.path.join(kwargs['samples_dir'], "oth_train_samples_{}_opt.h5".format(i + 1))
            oth_valid_smps_file = os.path.join(kwargs['samples_dir'], "oth_valid_samples_{}.h5".format(i + 1))

            out_cls_file = os.path.join(kwargs['out_cls_dir'], 'sen2_gfill_opt_xgb_cls_{}.json'.format(i + 1))

            c_dict = dict()
            c_dict['mng_train_smps_file'] = mng_train_smps_file
            c_dict['mng_valid_smps_file'] = mng_valid_smps_file
            c_dict['oth_train_smps_file'] = oth_train_smps_file
            c_dict['oth_valid_smps_file'] = oth_valid_smps_file
            c_dict['out_cls_file'] = out_cls_file
            self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(samples_dir='/scratch/a.pfb/gmw_v2_gapfill/data/set_samples_h5',
                              n_sample_sets=100,
                              out_cls_dir='/scratch/a.pfb/gmw_v2_gapfill/data/opt_cls_files')
        self.pop_params_db()
        self.create_slurm_sub_sh("find_cls_params_xgb", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

    def run_check_outputs(self):
        process_tools_mod = 'opt_xgb_cls_params'
        process_tools_cls = 'OptmiseXGBParams'
        time_sample_str = self.generate_readable_timestamp_str()
        out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
        out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
        self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)

    def run_remove_outputs(self, all_jobs=False, error_jobs=False):
        process_tools_mod = 'opt_xgb_cls_params'
        process_tools_cls = 'OptmiseXGBParams'
        self.remove_job_outputs(process_tools_mod, process_tools_cls, all_jobs, error_jobs)


if __name__ == "__main__":
    py_script = os.path.abspath("opt_xgb_cls_params.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'opt_xgb_cls_params'
    process_tools_cls = 'OptmiseXGBParams'

    create_tools = GenExtractSamplesCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/gmw_gap_fill_db/pbpt_db_conn.txt",
                                         lock_file_path="./gmw_gapfill_lock_file.txt",
                                         process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
