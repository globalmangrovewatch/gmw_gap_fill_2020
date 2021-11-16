
import os
import glob
import rsgislib.tools.filetools
import rsgislib.vectorutils

vec_files = glob.glob("/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_pts/*.gpkg")
out_base_dir = "/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_pts_sets"

for vec_file in vec_files:
    print(vec_file)
    out_vec_lyr = vec_lyr = rsgislib.vectorutils.get_vec_lyrs_lst(vec_file)[0]
    basename = rsgislib.tools.filetools.get_file_basename(vec_file)
    out_dir = os.path.join(out_base_dir, basename)
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    
    out_vec_file_base = os.path.join(out_dir, "{}_".format(basename))
    rsgislib.vectorutils.create_acc_pt_sets(vec_file, vec_lyr, out_vec_file_base, out_vec_lyr, "gmw_v25_cls", 20, sets_col="set_id", out_format='GeoJSON', out_ext='geojson', rnd_seed=42)
    