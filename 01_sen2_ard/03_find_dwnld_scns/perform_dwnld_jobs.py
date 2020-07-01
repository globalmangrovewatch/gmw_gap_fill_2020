from pbprocesstools.pbpt_q_process import PBPTQProcessTool
from sen2scnprocess import RecordSen2Process
import logging
import os

logger = logging.getLogger(__name__)

class PerformScnDownload(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='exe_scn_processing.py', descript=None)

    def do_processing(self, **kwargs):
        sen2_rcd_obj = RecordSen2Process(self.params['scn_db_file'])
        downloaded = sen2_rcd_obj.is_scn_downloaded(self.params['product_id'])
        if not downloaded:
            logger.debug("Using Google storage API to download.")
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.params['goog_key_json']
            os.environ["GOOGLE_CLOUD_PROJECT"] = self.params['goog_proj']
            from google.cloud import storage
            storage_client = storage.Client()

            logger.debug("Building download info for '{}'".format(self.params['scn_url']))
            url_path = self.params['scn_url']
            url_path = url_path.replace("gs://", "")
            url_path_parts = url_path.split("/")
            bucket_name = url_path_parts[0]
            if bucket_name != "gcp-public-data-sentinel-2":
                logger.error("Incorrect bucket name '" + bucket_name + "'")
                raise Exception("The bucket specified in the URL is not the Google Public Landsat Bucket"
                                          " - something has gone wrong.")
            bucket_prefix = url_path.replace(bucket_name + "/", "")
            dwnld_out_dirname = url_path_parts[-1]
            scn_lcl_dwnld_path = os.path.join(self.params['downpath'], dwnld_out_dirname)
            if not os.path.exists(scn_lcl_dwnld_path):
                os.mkdir(scn_lcl_dwnld_path)

            logger.debug("Get the storage bucket and blob objects.")
            bucket_obj = storage_client.get_bucket(bucket_name)
            bucket_blobs = bucket_obj.list_blobs(prefix=bucket_prefix)
            scn_dwnlds_filelst = list()
            for blob in bucket_blobs:
                if "$folder$" in blob.name:
                    continue
                scnfilename = blob.name.replace(bucket_prefix + "/", "")
                dwnld_file = os.path.join(scn_lcl_dwnld_path, scnfilename)
                dwnld_dirpath = os.path.split(dwnld_file)[0]
                if (not os.path.exists(dwnld_dirpath)):
                    os.makedirs(dwnld_dirpath, exist_ok=True)
                scn_dwnlds_filelst.append({"bucket_path": blob.name, "dwnld_path": dwnld_file})


            bucket_obj = storage_client.get_bucket(bucket_name)
            for dwnld in scn_dwnlds_filelst:
                blob_obj = bucket_obj.blob(dwnld["bucket_path"])
                blob_obj.download_to_filename(dwnld["dwnld_path"])

            sen2_rcd_obj.set_scn_downloaded(self.params['product_id'], self.params['downpath'])

    def required_fields(self, **kwargs):
        return ["product_id", "scn_url", "downpath", "scn_db_file", "goog_key_json", "goog_proj"]

    def outputs_present(self, **kwargs):
        sen2_rcd_obj = RecordSen2Process(self.params['scn_db_file'])
        downloaded = sen2_rcd_obj.is_scn_downloaded(self.params['product_id'])
        return downloaded, dict()

if __name__ == "__main__":
    PerformScnDownload().std_run()


