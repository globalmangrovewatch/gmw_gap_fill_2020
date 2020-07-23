from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import rsgislib.imageutils.imagecomp

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
        n_imgs = len(self.params['imgs'])

        if n_imgs > 1:
            rBand = 3
            nBand = 7
            sBand = 9

            outRefImg = os.path.join(self.params['comp_dir'], "sen2_comp_{}_refimg.kea".format(self.params['granule']))
            outCompImg = os.path.join(self.params['comp_dir'], "sen2_comp_{}_refl.kea".format(self.params['granule']))
            outMskImg = os.path.join(self.params['comp_dir'], "sen2_comp_{}_mskimg.kea".format(self.params['granule']))

            rsgislib.imageutils.imagecomp.createMaxNDVINDWIComposite(self.params['imgs'][0], self.params['imgs'],
                                                                     rBand, nBand, sBand, outRefImg,
                                                                     outCompImg, outMskImg, tmpPath=self.params['tmp_dir'],
                                                                     gdalformat='KEA', dataType=None, calcStats=True,
                                                                     reprojmethod='cubic', use_mode=True)
            outCompTIFImg = os.path.join(self.params['comp_tif_dir'], "sen2_comp_{}_refl.tif".format(self.params['granule']))
            gdal_translate_gtiff(outCompImg, outCompTIFImg)
        elif n_imgs == 1:
            outCompTIFImg = os.path.join(self.params['comp_tif_dir'], "sen2_comp_{}_refl.tif".format(self.params['granule']))
            gdal_translate_gtiff(self.params['imgs'][0], outCompTIFImg)


        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["granule", "imgs", "comp_dir", "comp_tif_dir", "tmp_dir"]

    def outputs_present(self, **kwargs):
        outCompTIFImg = os.path.join(self.params['comp_tif_dir'], "sen2_comp_{}_refl.tif".format(self.params['granule']))
        files_dict = dict()
        files_dict[outCompTIFImg] = 'gdal_image'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

        # Remove the output file.
        outCompTIFImg = os.path.join(self.params['comp_tif_dir'], "sen2_comp_{}_refl.tif".format(self.params['granule']))
        if os.path.exists(outCompTIFImg):
            os.remove(outCompTIFImg)

if __name__ == "__main__":
    ComputeSen2GranuleComposite().std_run()


