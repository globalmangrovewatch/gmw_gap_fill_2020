import glob
import os

import rsgislib.imagecalc
import rsgislib.tools.filetools
import rsgislib.classification
import rsgislib.vectorattrs

def create_acc_pts(input_img, out_vec_file, out_vec_lyr):
    n_mng_pxls, n_oth_pxls = rsgislib.imagecalc.count_pxls_of_val(input_img, [1,2])
    
    print("Mangrove: {}".format(n_mng_pxls))
    print("Other: {}".format(n_oth_pxls))
    
    min_pxls = n_mng_pxls
    if n_oth_pxls < n_mng_pxls:
        min_pxls = n_oth_pxls
    
    n_acc_pts = 2000
    
    if min_pxls < 1000:
        n_acc_pts = 500
    elif min_pxls < 2000:
        n_acc_pts = 1000
    elif min_pxls < 3000:
        n_acc_pts = 1500
        
    rsgislib.classification.generate_stratified_random_accuracy_pts(input_img, out_vec_file, out_vec_lyr, "GPKG", "cls_name", "gmw_v25_cls", "ref_cls", n_acc_pts, 42, True, True)
    rsgislib.vectorattrs.add_fid_col(out_vec_file, out_vec_lyr, out_vec_file, out_vec_lyr, out_format="GPKG", out_col="pt_id")





out_dir = "/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_pts"
imgs = glob.glob("/Users/pete/Temp/gmw_v25_extent/roi_cls_imgs/*.kea")
for img in imgs:
    basename = rsgislib.tools.filetools.get_file_basename(img)
    out_vec_file = os.path.join(out_dir, "{}_acc_pts.gpkg".format(basename))
    print(out_vec_file)
    out_vec_lyr = "{}_acc_pts".format(basename)
    create_acc_pts(img, out_vec_file, out_vec_lyr)
