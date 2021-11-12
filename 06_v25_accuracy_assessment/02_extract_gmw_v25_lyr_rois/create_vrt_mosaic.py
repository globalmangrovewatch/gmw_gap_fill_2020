import rsgislib.imageutils
import glob

input_imgs = glob.glob("/Users/pete/Temp/gmw_v25_extent/imgs/*.kea")

rsgislib.imageutils.create_mosaic_images_vrt(input_imgs, "/Users/pete/Temp/gmw_v25_extent/gmw_2010_extent_v25.vrt")
