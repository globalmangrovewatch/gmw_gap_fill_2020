from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib
import rsgislib.imagecalc
import rsgislib.vectorutils
import rsgislib.rastergis
import rsgislib.imageutils

import osgeo.ogr as ogr

logger = logging.getLogger(__name__)

class ExtractSceneTrainSamples(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='extract_scn_train_data.py', descript=None)

    def do_processing(self, **kwargs):
        # Rasterise the points.
        training_smpls_img = os.path.join(self.params['tmp_dir'], "{}_train_samples.kea".format(self.params['scn_id']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['samples_vec_file'], self.params['samples_vec_lyr'],
                                             self.params['vld_img'], training_smpls_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt="ClassID", vecExt=False,
                                             thematic=True, nodata=0)
        # Mask using the clear sky mask.
        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('smpls', training_smpls_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('csky', self.params['clrsky_img'], 1))
        training_smpls_mskd_img = os.path.join(self.params['tmp_dir'], "{}_train_samples_mskd.kea".format(self.params['scn_id']))
        rsgislib.imagecalc.bandMath(training_smpls_mskd_img, '(csky == 1)?smpls:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)

        # Extract the data
        fileInfo = [rsgislib.imageutils.ImageBandInfo(self.params['sref_img'], 'sen2', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]
        rsgislib.imageutils.extractZoneImageBandValues2HDF(fileInfo, training_smpls_mskd_img, self.params['scn_out_mng_file'], 1.0)
        rsgislib.imageutils.extractZoneImageBandValues2HDF(fileInfo, training_smpls_mskd_img, self.params['scn_out_oth_file'], 2.0)

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["scn_id", "vld_img", "clrsky_img", "sref_img", "samples_vec_file", "samples_vec_lyr",
                "scn_out_mng_file", "scn_out_oth_file", "tmp_dir"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['scn_out_mng_file']] = 'hdf5'
        files_dict[self.params['scn_out_oth_file']] = 'hdf5'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

        # Remove the output file.
        if os.path.exists(self.params['granule_out_vec_file']):
            os.remove(self.params['granule_out_vec_file'])
        if os.path.exists(self.params['granule_out_mng_img_file']):
            os.remove(self.params['granule_out_mng_img_file'])
        if os.path.exists(self.params['granule_out_oth_img_file']):
            os.remove(self.params['granule_out_oth_img_file'])

if __name__ == "__main__":
    ExtractSceneTrainSamples().std_run()


