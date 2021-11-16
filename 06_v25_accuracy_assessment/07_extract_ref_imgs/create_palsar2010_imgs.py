import glob

import rsgislib
import rsgislib.imageutils

rsgislib.imageutils.set_env_vars_lzw_gtiff_outs()


input_imgs = glob.glob("/mangroves_server/mangroves/global_mangrove_watch_original/Projects/2010/dBs/*.kea")
vrt_glb_img = "/bigdata/gmw_v25_acc_ass/gmw_palsar_2010.vrt"

rsgislib.imageutils.create_mosaic_images_vrt(input_imgs, vrt_glb_img)


vec_file = "../01_define_rois/roi_centre_bboxs_roi_ids.geojson"
vec_lyr = "roi_centre_bboxs_roi_ids"
out_img_base = "/bigdata/gmw_v25_acc_ass/palsar10_acc_rois/gmw_palsar10_acc_roi_"

rsgislib.imageutils.subset_to_geoms_bbox(vrt_glb_img, vec_file, vec_lyr, "roi_id", out_img_base, "GTIFF", None, "tif")

