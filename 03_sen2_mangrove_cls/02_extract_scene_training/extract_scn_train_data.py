from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import pathlib
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

        train_edit_mng_smpls_img = os.path.join(self.params['tmp_dir'], "{}_edit_mng_train_smps.kea".format(self.params['scn_id']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['samples_vec_edits'], self.params['mangrove_samp_lyrs'],
                                             self.params['vld_img'], train_edit_mng_smpls_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        train_edit_oth_smpls_img = os.path.join(self.params['tmp_dir'], "{}_edit_oth_train_smps.kea".format(self.params['scn_id']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['samples_vec_edits'], self.params['other_samp_lyrs'],
                                             self.params['vld_img'], train_edit_oth_smpls_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        train_edit_not_mng_img = os.path.join(self.params['tmp_dir'], "{}_edit_not_mng.kea".format(self.params['scn_id']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['samples_vec_edits'], self.params['not_mng_regions'],
                                             self.params['vld_img'], train_edit_not_mng_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        train_edit_not_oth_img = os.path.join(self.params['tmp_dir'], "{}_edit_not_oth.kea".format(self.params['scn_id']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['samples_vec_edits'], self.params['not_oth_regions'],
                                             self.params['vld_img'], train_edit_not_oth_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        train_water_smpls_img = os.path.join(self.params['tmp_dir'], "{}_water_smpls.kea".format(self.params['scn_id']))
        rsgislib.vectorutils.rasteriseVecLyr(self.params['water_smpls_vec_file'], self.params['water_smpls_vec_lyr'],
                                             self.params['vld_img'], train_water_smpls_img, gdalformat="KEA",
                                             burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                             thematic=True, nodata=0)

        training_smpls_rm_rgns_img = os.path.join(self.params['tmp_dir'], "{}_train_samples_rm_rgns.kea".format(self.params['scn_id']))
        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('smpls', training_smpls_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('not_mng', train_edit_not_mng_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('not_oth', train_edit_not_oth_img, 1))
        rsgislib.imagecalc.bandMath(training_smpls_rm_rgns_img, '(smpls == 1) && (not_mng == 0)?1:(smpls == 2) && (not_oth == 0)?2:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)

        training_smpls_fnl_img = os.path.join(self.params['tmp_dir'], "{}_train_samples_fnl.kea".format(self.params['scn_id']))
        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('smpls', training_smpls_rm_rgns_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('mng_pts', train_edit_mng_smpls_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('oth_pts', train_edit_oth_smpls_img, 1))
        rsgislib.imagecalc.bandMath(training_smpls_fnl_img, '(smpls == 1)||(mng_pts == 1)?1:(smpls == 2)||(oth_pts == 1)?2:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)

        # Mask using the clear sky mask.
        training_smpls_mskd_img = os.path.join(self.params['tmp_dir'], "{}_train_samples_mskd.kea".format(self.params['scn_id']))
        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('smpls', training_smpls_fnl_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('csky', self.params['clrsky_img'], 1))
        rsgislib.imagecalc.bandMath(training_smpls_mskd_img, '(csky == 1)?smpls:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)

        training_water_smpls_mskd_img = os.path.join(self.params['tmp_dir'], "{}_water_smpls_mskd.kea".format(self.params['scn_id']))
        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('smpls', train_water_smpls_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('csky', self.params['clrsky_img'], 1))
        rsgislib.imagecalc.bandMath(training_water_smpls_mskd_img, '(csky == 1)?smpls:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)

        n_smpls = rsgislib.imagecalc.countPxlsOfVal(training_smpls_mskd_img, vals=[1, 2])
        print(n_smpls)

        # Extract the data
        fileInfo = [rsgislib.imageutils.ImageBandInfo(self.params['sref_img'], 'sen2', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]
        if n_smpls[0] > 0:
            rsgislib.imageutils.extractZoneImageBandValues2HDF(fileInfo, training_smpls_mskd_img, self.params['scn_out_mng_file'], 1.0)
        if n_smpls[1] > 0:
            rsgislib.imageutils.extractZoneImageBandValues2HDF(fileInfo, training_smpls_mskd_img, self.params['scn_out_oth_file'], 2.0)

        n_wat_smpls = rsgislib.imagecalc.countPxlsOfVal(training_water_smpls_mskd_img, vals=[1])
        print(n_wat_smpls)
        if n_wat_smpls[0] > 0:
            rsgislib.imageutils.extractZoneImageBandValues2HDF(fileInfo, training_water_smpls_mskd_img, self.params['scn_out_wat_file'], 1.0)

        pathlib.Path(self.params['scn_out_cpl_file']).touch()

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["scn_id", "vld_img", "clrsky_img", "sref_img", "samples_vec_file", "samples_vec_lyr",
                "samples_vec_edits", "mangrove_samp_lyrs", "other_samp_lyrs", "not_mng_regions",
                "not_oth_regions", "water_smpls_vec_file", "water_smpls_vec_lyr", "scn_out_mng_file",
                "scn_out_oth_file", "scn_out_wat_file", "scn_out_cpl_file", "tmp_dir"]

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
        if os.path.exists(self.params['scn_out_mng_file']):
            os.remove(self.params['scn_out_mng_file'])
        if os.path.exists(self.params['scn_out_oth_file']):
            os.remove(self.params['scn_out_oth_file'])

if __name__ == "__main__":
    ExtractSceneTrainSamples().std_run()


