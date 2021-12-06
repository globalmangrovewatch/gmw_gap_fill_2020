import rsgislib
import rsgislib.imageutils


input_img = "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/11_create_v2_mosaic/gmw_2010_extent_v2.vrt"
vec_file = "../01_define_rois/roi_centre_bboxs_roi_ids.geojson"
vec_lyr = "roi_centre_bboxs_roi_ids"
out_img_base = "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/11_create_v2_mosaic/roi_v2_imgs/gmw_acc_roi_"


rsgislib.imageutils.subset_to_geoms_bbox(input_img, vec_file, vec_lyr, "roi_id", out_img_base, "KEA", rsgislib.TYPE_8UINT, "kea")



