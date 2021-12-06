import rsgislib.imageutils
import glob

input_imgs = glob.glob("/Users/pete/Dropbox/University/Research/Data/Mangroves/GMW/gmw_extent_v2_gtiff_tiles/gmw2010v2.0/*.tif")

rsgislib.imageutils.create_mosaic_images_vrt(input_imgs, "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/11_create_v2_mosaic/gmw_2010_extent_v2.vrt")
