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

class CreateGranuleTrainSamples(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='create_granule_train_samples.py', descript=None)

    def do_processing(self, **kwargs):
        if not os.path.exists(self.params['granule_veg_msk']):
            raise Exception("Vegetation mask does not exist!")

        rsgis_utils = rsgislib.RSGISPyUtils()

        # Check there is veg within the veg mask - pixel count.
        pxl_val_counts = rsgislib.imagecalc.countPxlsOfVal(self.params['granule_veg_msk'], vals=[1])

        if pxl_val_counts[0] > 100:
            # Rasterise the mangrove habitat mask
            mangrove_hab_img = os.path.join(self.params['tmp_dir'], "{}_mangrove_hab.kea".format(self.params['granule']))
            rsgislib.vectorutils.rasteriseVecLyr(self.params['gmw_hab_msk_vec'], self.params['gmw_hab_msk_lyr'],
                                                 self.params['granule_veg_msk'], mangrove_hab_img, gdalformat="KEA",
                                                 burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                                 thematic=True, nodata=0)

            # Rasterise the mangrove mask
            mangrove_msk_img = os.path.join(self.params['tmp_dir'], "{}_mangrove_msk.kea".format(self.params['granule']))
            rsgislib.vectorutils.rasteriseVecLyr(self.params['gmw_msk_vec'], self.params['gmw_msk_lyr'],
                                                 self.params['granule_veg_msk'], mangrove_msk_img, gdalformat="KEA",
                                                 burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt=None, vecExt=False,
                                                 thematic=True, nodata=0)
            # Perform buffer.
            non_mng_dist_img = os.path.join(self.params['tmp_dir'], "{}_non_mng_dist.kea".format(self.params['granule']))
            rsgislib.imagecalc.calcDist2ImgVals(mangrove_msk_img, non_mng_dist_img, [0], valsImgBand=1, gdalformat='KEA',
                                                maxDist=5, noDataVal=None, unitGEO=False)
            mang_smpl_msk_img = os.path.join(self.params['tmp_dir'], "{}_mng_smpl_msk.kea".format(self.params['granule']))
            rsgislib.imagecalc.imageMath(non_mng_dist_img, mang_smpl_msk_img, 'b1>2', 'KEA', rsgislib.TYPE_8UINT)

            # Define non-mangrove regions
            non_mangrove_img = os.path.join(self.params['tmp_dir'], "{}_non_mangrove.kea".format(self.params['granule']))
            bandDefns = []
            bandDefns.append(rsgislib.imagecalc.BandDefn('hab', mangrove_hab_img, 1))
            bandDefns.append(rsgislib.imagecalc.BandDefn('veg', self.params['granule_veg_msk'], 1))
            rsgislib.imagecalc.bandMath(non_mangrove_img, '(veg == 1) && (hab == 0)?1:0', 'KEA',
                                        rsgislib.TYPE_8UINT, bandDefns)

            # Define the training samples.
            mang_pxl_count = rsgislib.imagecalc.countPxlsOfVal(mang_smpl_msk_img, vals=[1])[0]
            if mang_pxl_count < 1000:
                rsgislib.imagecalc.imageMath(mang_smpl_msk_img, self.params['granule_out_mng_img_file'],
                                             'b1', 'KEA', rsgislib.TYPE_8UINT)
            elif mang_pxl_count > 2500:
                rsgislib.imageutils.performRandomPxlSampleInMaskLowPxlCount(mang_smpl_msk_img,
                                                                            self.params['granule_out_mng_img_file'],
                                                                            'KEA', maskvals=1,
                                                                             numSamples=2500, rndSeed=42)
            else:
                rsgislib.imageutils.performRandomPxlSampleInMaskLowPxlCount(mang_smpl_msk_img,
                                                                            self.params['granule_out_mng_img_file'],
                                                                            'KEA', maskvals=1,
                                                                             numSamples=1000, rndSeed=42)

            non_mang_pxl_count = rsgislib.imagecalc.countPxlsOfVal(non_mangrove_img, vals=[1])[0]
            if non_mang_pxl_count < 1000:
                rsgislib.imagecalc.imageMath(non_mangrove_img, self.params['granule_out_oth_img_file'],
                                             'b1', 'KEA', rsgislib.TYPE_8UINT)
            elif non_mang_pxl_count > 2500:
                rsgislib.imageutils.performRandomPxlSampleInMaskLowPxlCount(non_mangrove_img,
                                                                            self.params['granule_out_oth_img_file'],
                                                                            'KEA', maskvals=1,
                                                                            numSamples=2500, rndSeed=42)
            else:
                rsgislib.imageutils.performRandomPxlSampleInMaskLowPxlCount(non_mangrove_img,
                                                                            self.params['granule_out_oth_img_file'],
                                                                            'KEA', maskvals=1,
                                                                            numSamples=1000, rndSeed=42)


            # Vectorise Mangrove Training Points
            mang_smpl_pxl_count = rsgislib.imagecalc.countPxlsOfVal(self.params['granule_out_mng_img_file'], vals=[1])[0]
            if mang_smpl_pxl_count > 0:
                mangrove_train_pts_vec = os.path.join(self.params['tmp_dir'],
                                                "{}_mangrove_train_pts.geojson".format(self.params['granule']))
                mangrove_train_pts_lyr = '{}_mangrove_train_pts'.format(self.params['granule'])
                rsgislib.vectorutils.exportPxls2Pts(self.params['granule_out_mng_img_file'], mangrove_train_pts_vec, 1,
                                                    False, mangrove_train_pts_lyr, 'GEOJSON')

                n_mang_feats = rsgislib.vectorutils.getVecFeatCount(mangrove_train_pts_vec, mangrove_train_pts_lyr)
                granule_names = list()
                class_name = list()
                for i in range(n_mang_feats):
                    granule_names.append(self.params['granule'])
                    class_name.append('mangrove')
                rsgislib.vectorutils.writeVecColumn(mangrove_train_pts_vec, mangrove_train_pts_lyr, 'Granule',
                                                    ogr.OFTString, granule_names)
                rsgislib.vectorutils.writeVecColumn(mangrove_train_pts_vec, mangrove_train_pts_lyr, 'Class',
                                                    ogr.OFTString, class_name)

            # Vectorise Other Training Points
            other_smpl_pxl_count = rsgislib.imagecalc.countPxlsOfVal(self.params['granule_out_oth_img_file'], vals=[1])[0]
            if other_smpl_pxl_count > 0:
                other_train_pts_vec = os.path.join(self.params['tmp_dir'],
                                                      "{}_other_train_pts.geojson".format(self.params['granule']))
                other_train_pts_lyr = '{}_other_train_pts'.format(self.params['granule'])
                rsgislib.vectorutils.exportPxls2Pts(self.params['granule_out_oth_img_file'], other_train_pts_vec, 1,
                                                    False, other_train_pts_lyr, 'GEOJSON')

                n_other_feats = rsgislib.vectorutils.getVecFeatCount(other_train_pts_vec, other_train_pts_lyr)
                granule_names = list()
                class_name = list()
                for i in range(n_other_feats):
                    granule_names.append(self.params['granule'])
                    class_name.append('other')
                rsgislib.vectorutils.writeVecColumn(other_train_pts_vec, other_train_pts_lyr, 'Granule',
                                                    ogr.OFTString, granule_names)
                rsgislib.vectorutils.writeVecColumn(other_train_pts_vec, other_train_pts_lyr, 'Class',
                                                    ogr.OFTString, class_name)

            if (mang_smpl_pxl_count > 0) and (other_smpl_pxl_count > 0):
                rsgislib.vectorutils.merge_vector_files([mangrove_train_pts_vec, other_train_pts_vec],
                                                        self.params['granule_out_vec_file'],
                                                        output_lyr='samples', out_format='GPKG', out_epsg=None)
            elif mang_smpl_pxl_count > 0:
                rsgislib.vectorutils.merge_vector_files([mangrove_train_pts_vec],
                                                        self.params['granule_out_vec_file'],
                                                        output_lyr='samples', out_format='GPKG', out_epsg=None)
            elif other_smpl_pxl_count > 0:
                rsgislib.vectorutils.merge_vector_files([other_train_pts_vec],
                                                        self.params['granule_out_vec_file'],
                                                        output_lyr='samples', out_format='GPKG', out_epsg=None)

        #if os.path.exists(self.params['tmp_dir']):
        #    shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["granule", "gmw_msk_vec", "gmw_msk_lyr", "gmw_hab_msk_vec", "gmw_hab_msk_lyr", "granule_veg_msk",
                "granule_out_vec_file", "granule_out_mng_img_file", "granule_out_oth_img_file", "tmp_dir"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['granule_out_mng_img_file']] = 'gdal_image'
        files_dict[self.params['granule_out_oth_img_file']] = 'gdal_image'
        files_dict[self.params['granule_out_vec_file']] = 'gdal_vector'
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
    CreateGranuleTrainSamples().std_run()


