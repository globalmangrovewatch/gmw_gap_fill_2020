from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import rsgislib
import logging
import os
import sys

sys.path.insert(0, "../03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class GenGranuleChkCoverage(PBPTGenQProcessToolCmds):

    def find_first_file(self, dirPath, fileSearch, rtn_except=True):
        """
        Search for a single file with a path using glob. Therefore, the file
        path returned is a true path. Within the fileSearch provide the file
        name with '*' as wildcard(s).
        :param dirPath:
        :param fileSearch:
        :return:
        """
        import glob
        files = None
        for root, dirs, files in os.walk(dirPath):
            files = glob.glob(os.path.join(root, fileSearch))
            if len(files) > 0:
                break
        out_file = None
        if (files is not None) and (len(files) == 1):
            out_file = files[0]
        elif rtn_except:
            raise Exception("Could not find a single file ({0}) in {1}; "
                            "found {2} files.".format(fileSearch, dirPath, len(files)))
        return out_file

    def gen_command_info(self, **kwargs):
        if not os.path.exists(kwargs['scn_db_file']):
            raise Exception("Sentinel-2 scene database does not exist...")

        rsgis_utils = rsgislib.RSGISPyUtils()
        granule_lst = rsgis_utils.readTextFile2List(kwargs['granule_lst'])

        sen2_rcd_obj = RecordSen2Process(kwargs['scn_db_file'])
        err_scns = []
        for granule in granule_lst:
            print(granule)
            granule_out_file = os.path.join(kwargs['granule_out_file'], "{}.json".format(granule))
            if not os.path.exists(granule_out_file):
                scns = sen2_rcd_obj.granule_scns(granule)
                vld_imgs = list()
                clrsky_imgs = list()
                for scn in scns:
                    print("\t{}".format(scn.product_id))
                    if scn.ard:
                        vld_img = self.find_first_file(scn.ard_path, "*valid.kea", rtn_except=False)
                        clrsky_img = self.find_first_file(scn.ard_path, "*clearsky.kea", rtn_except=False)
                        if (vld_img is None) and (clrsky_img is None):
                            clouds_img = self.find_first_file(scn.ard_path, "*clouds.kea", rtn_except=False)
                            if clouds_img is None:
                                print("***ERROR***: {}".format(scn.ard_path))
                                err_scns.append(scn.ard_path)
                        else:
                            vld_imgs.append(vld_img)
                            clrsky_imgs.append(clrsky_img)
                if (len(vld_imgs) > 0) and (len(clrsky_imgs) > 0):
                    c_dict = dict()
                    c_dict['granule'] = granule
                    c_dict['vld_imgs'] = vld_imgs
                    c_dict['clrsky_imgs'] = clrsky_imgs
                    c_dict['roi_vec_file'] = kwargs['roi_vec_file']
                    c_dict['roi_vec_lyr'] = kwargs['roi_vec_lyr']
                    c_dict['granule_out_file'] = granule_out_file
                    c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], "{}_chk_cover".format(granule))
                    if not os.path.exists(c_dict['tmp_dir']):
                        os.mkdir(c_dict['tmp_dir'])
                    self.params.append(c_dict)
        print("ERRORS:")
        for err_scn in err_scns:
            print(err_scn)

    def run_gen_commands(self):
        self.gen_command_info(scn_db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db',
                              granule_lst='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/sen2_roi_granule_lst.txt',
                              roi_vec_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/00_define_rois/gmw_missing_regions.geojson',
                              roi_vec_lyr='gmw_missing_regions',
                              granule_out_path='/scratch/a.pfb/gmw_v2_gapfill/data/granule_cover',
                              tmp_dir='/scratch/a.pfb/gmw_v2_gapfill/tmp')
        self.pop_params_db()
        self.create_slurm_sub_sh("sen2_granule_comps", 16448, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

    def run_check_outputs(self):
        process_tools_mod = 'perform_coverage_chk'
        process_tools_cls = 'PerformCoverageCheck'
        time_sample_str = self.generate_readable_timestamp_str()
        out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
        out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
        self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)


if __name__ == "__main__":
    py_script = os.path.abspath("perform_coverage_chk.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    create_tools = GenGranuleChkCoverage(cmd=script_cmd, sqlite_db_file="sen2_granule_chk_cover.db")
    create_tools.parse_cmds()
