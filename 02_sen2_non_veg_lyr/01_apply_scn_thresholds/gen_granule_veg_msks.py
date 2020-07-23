from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import rsgislib
import logging
import os
import sys

sys.path.insert(0, "../../01_sen2_ard/03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class GenVegGranuleMsk(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        if not os.path.exists(kwargs['scn_db_file']):
            raise Exception("Sentinel-2 scene database does not exist...")

        rsgis_utils = rsgislib.RSGISPyUtils()
        granule_lst = rsgis_utils.readTextFile2List(kwargs['granule_lst'])

        sen2_rcd_obj = RecordSen2Process(kwargs['scn_db_file'])
        err_scns = []
        for granule in granule_lst:
            print(granule)
            granule_out_img_file = os.path.join(kwargs['granule_out_img_path'], "{}_veg.kea".format(granule))
            granule_out_vec_file = os.path.join(kwargs['granule_out_vec_path'], "{}_veg.gpkg".format(granule))
            if not os.path.exists(granule_out_img_file):
                scns = sen2_rcd_obj.granule_scns(granule)
                vld_imgs = list()
                clrsky_imgs = list()
                sref_imgs = list()
                for scn in scns:
                    print("\t{}".format(scn.product_id))
                    if scn.ard:
                        vld_img = self.find_first_file(scn.ard_path, "*valid.kea", rtn_except=False)
                        clrsky_img = self.find_first_file(scn.ard_path, "*clearsky.kea", rtn_except=False)
                        sref_img = self.find_first_file(scn.ard_path, "*vmsk_mclds_clearsky_topshad_rad_srefdem_stdsref.kea", rtn_except=False)
                        if (vld_img is None) or (clrsky_img is None) or (sref_img is None):
                            clouds_img = self.find_first_file(scn.ard_path, "*clouds.kea", rtn_except=False)
                            if clouds_img is None:
                                print("***ERROR***: {}".format(scn.ard_path))
                                err_scns.append(scn.ard_path)
                        else:
                            vld_imgs.append(vld_img)
                            clrsky_imgs.append(clrsky_img)
                            sref_imgs.append(sref_img)
                if (len(vld_imgs) > 0) and (len(clrsky_imgs) > 0) and (len(sref_imgs) > 0):
                    c_dict = dict()
                    c_dict['granule'] = granule
                    c_dict['dem_file'] = kwargs['dem_file']
                    c_dict['water_file'] = kwargs['water_file']
                    c_dict['vld_imgs'] = vld_imgs
                    c_dict['clrsky_imgs'] = clrsky_imgs
                    c_dict['sref_imgs'] = sref_imgs
                    c_dict['granule_out_lyr'] = granule
                    c_dict['granule_out_vec_file'] = granule_out_vec_file
                    c_dict['granule_out_img_file'] = granule_out_img_file
                    c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], "{}_granule_veg_msk".format(granule))
                    if not os.path.exists(c_dict['tmp_dir']):
                        os.mkdir(c_dict['tmp_dir'])
                    self.params.append(c_dict)
        print("ERRORS:")
        for err_scn in err_scns:
            print(err_scn)

    def run_gen_commands(self):
        self.gen_command_info(scn_db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db',
                              granule_lst='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/sen2_roi_granule_lst.txt',
                              dem_file='/scratch/a.pfb/srtm_global_mosaic_1arc_v3.kea',
                              water_file='/scratch/a.pfb/water_occurence/water_occurence.kea',
                              granule_out_img_path='/scratch/a.pfb/gmw_v2_gapfill/data/granule_vegmsks_imgs',
                              granule_out_vec_path='/scratch/a.pfb/gmw_v2_gapfill/data/granule_vegmsks_vecs',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("sen2_granule_veg", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

    def run_check_outputs(self):
        process_tools_mod = 'create_granule_veg_msk'
        process_tools_cls = 'CreateGranuleVegMsk'
        time_sample_str = self.generate_readable_timestamp_str()
        out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
        out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
        self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)

    def run_remove_outputs(self, all_jobs=False, error_jobs=False):
        process_tools_mod = 'create_granule_veg_msk'
        process_tools_cls = 'CreateGranuleVegMsk'
        self.remove_job_outputs(process_tools_mod, process_tools_cls, all_jobs, error_jobs)


if __name__ == "__main__":
    py_script = os.path.abspath("create_granule_veg_msk.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    create_tools = GenVegGranuleMsk(cmd=script_cmd, sqlite_db_file="sen2_granule_veg_msks.db")
    create_tools.parse_cmds()
