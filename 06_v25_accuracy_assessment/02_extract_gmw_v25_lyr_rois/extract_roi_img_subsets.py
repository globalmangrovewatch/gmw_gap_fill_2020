import rsgislib
import rsgislib.imageutils


input_img = "/Users/pete/Temp/gmw_v25_extent/gmw_2010_extent_v25.vrt"
vec_file = "../01_define_rois/roi_centre_bboxs_roi_ids.geojson"
vec_lyr = "roi_centre_bboxs_roi_ids"
out_img_base = "/Users/pete/Temp/gmw_v25_extent/roi_imgs/gmw_acc_roi_"


rsgislib.imageutils.subset_to_geoms_bboxs(input_img, vec_file, vec_lyr, "roi_id", out_img_base, "KEA", rsgislib.TYPE_8UINT, "kea")


