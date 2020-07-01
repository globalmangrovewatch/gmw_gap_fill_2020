from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import rsgislib
import os.path
import logging
import sqlite3
import statistics
import os
from sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class FindSen2ScnsGenDwnlds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        rsgis_utils = rsgislib.RSGISPyUtils()
        granule_lst = rsgis_utils.readTextFile2List(kwargs['granule_lst'])
        gg_sen2_db_conn = sqlite3.connect(kwargs['db_file'])
        query = """SELECT PRODUCT_ID, BASE_URL FROM SEN2 WHERE MGRS_TILE = ? AND CLOUD_COVER < ? 
                   AND date(SENSING_TIME) > date(?) AND date(SENSING_TIME) < date(?) 
                   AND GEOMETRIC_QUALITY_FLAG = 0 AND CAST(TOTAL_SIZE as decimal) > ? 
                   ORDER BY CLOUD_COVER ASC LIMIT {}""".format(kwargs['n_scns'])

        query_total_size = """SELECT TOTAL_SIZE FROM SEN2 WHERE MGRS_TILE = ? AND CLOUD_COVER < ? 
                              AND date(SENSING_TIME) > date(?) AND date(SENSING_TIME) < date(?)"""

        sen2_rcd_obj = RecordSen2Process(kwargs['scn_db_file'])
        if not os.path.exists(kwargs['scn_db_file']):
            sen2_rcd_obj.init_db()

        for granule in granule_lst:
            logger.info("Processing: {}".format(granule))
            n_scns = sen2_rcd_obj.n_granule_scns(granule)
            if n_scns < kwargs['n_scns']:
                n_xt_scns = kwargs['n_scns'] - n_scns
                gg_sen2_db_cursor = gg_sen2_db_conn.cursor()
                query_ts_vars = [granule, kwargs['cloud_thres_ts'], kwargs['start_date'], kwargs['end_date']]
                total_size_lst = list()
                for row in gg_sen2_db_cursor.execute(query_total_size, query_ts_vars):
                    total_size_lst.append(float(row[0]))
                ts_mean = statistics.mean(total_size_lst)
                ts_stdev = statistics.stdev(total_size_lst)
                ts_thres = ts_mean - ts_stdev
                logger.debug("Total Size Threshold: {}".format(ts_thres))
                query_vars = [granule, kwargs['cloud_thres'], kwargs['start_date'], kwargs['end_date'], ts_thres]
                scn_lst = list()
                for row in gg_sen2_db_cursor.execute(query, query_vars):
                    print(row[0])
                    if not sen2_rcd_obj.is_scn_in_db(row[0]):
                        scn = dict()
                        scn['product_id'] = row[0]
                        scn['scn_url'] = row[1]
                        scn['granule'] = granule
                        scn_lst.append(scn)

                        c_dict = dict()
                        c_dict['product_id'] = row[0]
                        c_dict['scn_url'] = row[1]
                        c_dict['downpath'] = os.path.join(kwargs['cloud_thres_ts'], row[0])
                        c_dict['scn_db_file'] = kwargs['scn_db_file']
                        c_dict['goog_key_json'] = kwargs['goog_key_json']
                        if not os.path.exists(c_dict['downpath']):
                            os.mkdir(c_dict['downpath'])
                        self.params.append(c_dict)
                        n_scns += 1
                    if n_scns >= kwargs['n_scns']:
                        break
                if len(scn_lst) > 0:
                    sen2_rcd_obj.add_sen2_scns(scn_lst)

    def run_gen_commands(self):
        self.gen_command_info(db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_db_20200701.db',
                              granule_lst='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/sen2_roi_granule_lst.txt',
                              cloud_thres=20,
                              cloud_thres_ts=50,
                              start_date='2016-01-01',
                              end_date='2020-07-01',
                              n_scns=10,
                              scn_db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db',
                              dwnld_path='/scratch/a.pfb/gmw_v2_gapfill/data/dwnlds',
                              goog_key_json='/home/a.pfb/eodd_gmw_info/GlobalMangroveWatch-74b58b05fd73.json')
        self.pop_params_db()
        self.create_slurm_sub_sh("dwnld_sen2_scns", 8224, '/scratch/a.pfb/gmw_v2_gapfill/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=5, n_jobs=2,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

    def run_check_outputs(self):
        process_tools_mod = 'exe_scn_processing'
        process_tools_cls = 'ProcessEODDScn'
        time_sample_str = self.generate_readable_timestamp_str()
        out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
        out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
        self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)


if __name__ == "__main__":
    py_script = os.path.abspath("perform_dwnld_jobs.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    create_tools = FindSen2ScnsGenDwnlds(cmd=script_cmd, sqlite_db_file="dwnld_sen2_scns.db")
    create_tools.parse_cmds()
