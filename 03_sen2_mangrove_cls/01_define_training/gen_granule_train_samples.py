from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import rsgislib
import logging
import os
import sys

logger = logging.getLogger(__name__)

class GenGranuleTrainSamples(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        rsgis_utils = rsgislib.RSGISPyUtils()
        granule_lst = rsgis_utils.readTextFile2List(kwargs['granule_lst'])

        for granule in granule_lst:
            print(granule)
            granule_out_mng_img_file = os.path.join(kwargs['granule_out_img_path'], "{}_mng_train_smpls.kea".format(granule))
            granule_out_oth_img_file = os.path.join(kwargs['granule_out_img_path'], "{}_oth_train_smpls.kea".format(granule))
            granule_out_vec_file = os.path.join(kwargs['granule_out_vec_path'], "{}_train_smpls.gpkg".format(granule))
            granule_veg_img_file = os.path.join(kwargs['granule_veg_msks_dir'], "{}_veg.kea".format(granule))
            if ((not os.path.exists(granule_out_mng_img_file)) or (not os.path.exists(granule_out_oth_img_file))) and os.path.exists(granule_veg_img_file):
                c_dict = dict()
                c_dict['granule'] = granule
                c_dict['gmw_msk_vec'] = kwargs['gmw_msk_vec']
                c_dict['gmw_msk_lyr'] = kwargs['gmw_msk_lyr']
                c_dict['gmw_hab_msk_vec'] = kwargs['gmw_hab_msk_vec']
                c_dict['gmw_hab_msk_lyr'] = kwargs['gmw_hab_msk_lyr']
                c_dict['granule_veg_msk'] = granule_veg_img_file
                c_dict['granule_out_vec_file'] = granule_out_vec_file
                c_dict['granule_out_mng_img_file'] = granule_out_mng_img_file
                c_dict['granule_out_oth_img_file'] = granule_out_oth_img_file
                c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], "{}_granule_gen_train".format(granule))
                if not os.path.exists(c_dict['tmp_dir']):
                    os.mkdir(c_dict['tmp_dir'])
                self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(granule_lst='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/sen2_roi_granule_lst.txt',
                              granule_veg_msks_dir='/scratch/a.pfb/gmw_v2_gapfill/data/granule_vegmsks_imgs',
                              gmw_hab_msk_vec='/scratch/a.pfb/gmw_v2_gapfill/data/GMW_Mangrove_Habitat_v4.gpkg',
                              gmw_hab_msk_lyr='gmw_hab_v4',
                              gmw_msk_vec='/scratch/a.pfb/gmw_v2_gapfill/data/GMW_MangroveExtent_WGS84_v2.0.gpkg',
                              gmw_msk_lyr='gmw2010v2.0',
                              granule_out_img_path='/scratch/a.pfb/gmw_v2_gapfill/data/granule_mang_train_imgs',
                              granule_out_vec_path='/scratch/a.pfb/gmw_v2_gapfill/data/granule_mang_train_vecs',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("sen2_granule_mngtrain", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

    def run_check_outputs(self):
        process_tools_mod = 'create_granule_train_samples'
        process_tools_cls = 'CreateGranuleTrainSamples'
        time_sample_str = self.generate_readable_timestamp_str()
        out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
        out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
        self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)

    def run_remove_outputs(self, all_jobs=False, error_jobs=False):
        process_tools_mod = 'create_granule_train_samples'
        process_tools_cls = 'CreateGranuleTrainSamples'
        self.remove_job_outputs(process_tools_mod, process_tools_cls, all_jobs, error_jobs)


if __name__ == "__main__":
    py_script = os.path.abspath("create_granule_train_samples.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    create_tools = GenGranuleTrainSamples(cmd=script_cmd, sqlite_db_file="granule_define_train_samples.db")
    create_tools.parse_cmds()
