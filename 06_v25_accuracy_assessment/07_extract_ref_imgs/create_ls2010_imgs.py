import glob

import rsgislib
import rsgislib.imageutils

rsgislib.imageutils.set_env_vars_lzw_gtiff_outs()


input_imgs = glob.glob("/mangroves_server/mangroves/global_mangrove_watch_original/Projects/LandsatCompsMskedTIF/*.tif")
vrt_glb_img = "/bigdata/gmw_v25_acc_ass/gmw_landsat.vrt"

rsgislib.imageutils.create_mosaic_images_vrt(input_imgs, vrt_glb_img)


vec_file = "../01_define_rois/roi_centre_bboxs_roi_ids.geojson"
vec_lyr = "roi_centre_bboxs_roi_ids"
out_img_base = "/bigdata/gmw_v25_acc_ass/landsat_acc_rois/gmw_ls_acc_roi_"

rsgislib.imageutils.subset_to_geoms_bbox(vrt_glb_img, vec_file, vec_lyr, "roi_id", out_img_base, "GTIFF", rsgislib.TYPE_8UINT, "tif")


    