from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib.imageutils.imagecomp
import rsgislib.imageutils
import rsgislib.imagecalc

logger = logging.getLogger(__name__)

def gdal_translate_gtiff(input_img, output_img):
    """
    Using GDAL translate to convert input image to a different format, if GTIFF selected
    and no options are provided then a cloud optimised GeoTIFF will be outputted.

    :param input_img: Input image which is GDAL readable.
    :param output_img: The output image file.
    :param gdal_format: The output image file format
    :param options: options for the output driver (e.g., "-co TILED=YES -co COMPRESS=LZW -co BIGTIFF=YES")
    """
    import osgeo.gdal as gdal
    options = "-co TILED=YES -co INTERLEAVE=PIXEL -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -co COMPRESS=LZW -co BIGTIFF=YES -co COPY_SRC_OVERVIEWS=YES"

    try:
        import tqdm
        pbar = tqdm.tqdm(total=100)
        callback = lambda *args, **kw: pbar.update()
    except:
        callback = gdal.TermProgress

    trans_opt = gdal.TranslateOptions(format='GTIFF', options=options, callback=callback)
    gdal.Translate(output_img, input_img, options=trans_opt)


class ComputeSen2GranuleComposite(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='comp_composite.py', descript=None)

    def do_processing(self, **kwargs):
        mskd_imgs = []
        for img in self.params['imgs']:
            base_dir = os.path.dirname(img)
            basename = self.get_file_basename(img)
            clearsky_msk_img = self.find_first_file(base_dir, "*clearsky_refine.kea", False)
            valid_msk_img = self.find_first_file(base_dir, "*valid.kea", False)
            if clearsky_msk_img is not None:
                prop_useful = rsgislib.imagecalc.calcPropTrueExp("msk==1?1:0", [rsgislib.imagecalc.BandDefn('msk', clearsky_msk_img, 1)], valid_msk_img)
                if prop_useful > 0.05:
                    clrsky_mskd_img = os.path.join(self.params['tmp_dir'], "{}_clrsky_mskd.kea".format(basename))
                    rsgislib.imageutils.maskImage(img, clearsky_msk_img, clrsky_mskd_img, 'KEA', rsgislib.TYPE_16UINT, 0.0, 0)
                    mskd_imgs.append(clrsky_mskd_img)


        n_imgs = len(mskd_imgs)
        if n_imgs > 1:
            rBand = 3
            nBand = 7
            sBand = 9

            rsgislib.imageutils.imagecomp.createMaxNDVINDWIComposite(mskd_imgs[0], mskd_imgs,
                                                                     rBand, nBand, sBand,
                                                                     self.params['comp_out_ref_img'],
                                                                     self.params['comp_out_refl_img'],
                                                                     self.params['comp_out_msk_img'],
                                                                     tmpPath=self.params['tmp_dir'],
                                                                     gdalformat='KEA', dataType=None, calcStats=True,
                                                                     reprojmethod='cubic', use_mode=True)
            outCompTIFImg = os.path.join(self.params['comp_tif_dir'], "sen2_comp_{}_refl.tif".format(self.params['granule']))
            gdal_translate_gtiff(self.params['comp_out_refl_img'], outCompTIFImg)
        elif n_imgs == 1:
            outCompTIFImg = os.path.join(self.params['comp_tif_dir'], "sen2_comp_{}_refl.tif".format(self.params['granule']))
            gdal_translate_gtiff(mskd_imgs[0], outCompTIFImg)

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["granule", "imgs", "comp_out_ref_img", "comp_out_refl_img", "comp_out_msk_img",
                "comp_out_tif", "tmp_dir"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['comp_out_tif']] = 'gdal_image'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

        # Remove the output file.
        if os.path.exists(self.params['comp_out_tif']):
            os.remove(self.params['comp_out_tif'])

        if os.path.exists(self.params['comp_out_refl_img']):
            os.remove(self.params['comp_out_refl_img'])
        if os.path.exists(self.params['comp_out_ref_img']):
            os.remove(self.params['comp_out_ref_img'])
        if os.path.exists(self.params['comp_out_msk_img']):
            os.remove(self.params['comp_out_msk_img'])

if __name__ == "__main__":
    ComputeSen2GranuleComposite().std_run()


