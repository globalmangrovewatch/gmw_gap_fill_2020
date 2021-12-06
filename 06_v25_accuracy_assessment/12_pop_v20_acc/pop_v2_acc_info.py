import os
import rsgislib.classification


def pop_acc_pts(input_img, vec_file, vec_lyr):
    rsgislib.classification.pop_class_info_accuracy_pts(input_img, vec_file, vec_lyr, "cls_name", "gmw_v2_cls")



pts_dir = "../10_calc_set_acc_stats/set_acc_stats/"
imgs_dir = "../11_create_v2_mosaic/roi_v2_cls_imgs"
for i in range(60):
    vec_acc_pts_file = os.path.join(pts_dir, "gmw_v25_set_{}_acc_pts.geojson".format(i+1))
    vec_acc_pts_lyr = "gmw_v25_set_{}_acc_pts".format(i+1)
    
    img = os.path.join(imgs_dir, "gmw_acc_roi_{}_cls.kea".format(i+1))
    if os.path.exists(vec_acc_pts_file):
        pop_acc_pts(img, vec_acc_pts_file, vec_acc_pts_lyr)
