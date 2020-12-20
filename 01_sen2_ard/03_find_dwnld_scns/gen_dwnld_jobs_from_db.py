from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import rsgislib
import os.path
import logging
import sqlite3
import statistics
import os
from sen2scnprocess import RecordSen2Process

logger = logging.getLogger(__name__)

class Sen2ScnsGenDwnlds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        sen2_rcd_obj = RecordSen2Process(kwargs['scn_db_file'])
        scns = sen2_rcd_obj.get_scns_download()
        for scn in scns:
            c_dict = dict()
            c_dict['product_id'] = scn.product_id
            c_dict['scn_url'] = scn.scn_url
            c_dict['downpath'] = os.path.join(kwargs['dwnld_path'], scn.product_id)
            c_dict['scn_db_file'] = kwargs['scn_db_file']
            c_dict['goog_key_json'] = kwargs['goog_key_json']
            if not os.path.exists(c_dict['downpath']):
                os.mkdir(c_dict['downpath'])
            self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(
                scn_db_file='/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db',
                dwnld_path='/scratch/a.pfb/gmw_v2_gapfill/data/dwnlds',
                goog_key_json='/home/a.pfb/eodd_gmw_info/GlobalMangroveWatch-74b58b05fd73.json')

        self.pop_params_db()
        self.create_shell_exe("run_dwnld_cmds.sh", "dwnld_cmds.sh", 2, db_info_file=None)


if __name__ == "__main__":
    py_script = os.path.abspath("perform_dwnld_jobs.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'perform_dwnld_jobs'
    process_tools_cls = 'PerformScnDownload'

    create_tools = Sen2ScnsGenDwnlds(cmd=script_cmd, sqlite_db_file="dwnld_sen2_scns.db",
                                     lock_file_path="./gmw_gapfill_lock_file.txt",
                                     process_tools_mod=process_tools_mod,
                                     process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()
