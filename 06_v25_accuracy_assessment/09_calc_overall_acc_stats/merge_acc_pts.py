import os
import glob
import rsgislib.vectorutils
import rsgislib.vectorattrs

vec_files = []
acc_pts_base_dir = "../00_acc_pt_sets"
vecs_dict = list()
for i in range(60):
    acc_pts_dir = os.path.join(acc_pts_base_dir, "gmw_acc_roi_{}_cls_acc_pts".format(i+1))
    acc_pt_files = glob.glob(os.path.join(acc_pts_dir, "*.geojson"))
    for acc_pt_file in acc_pt_files:
        vec_lyr = rsgislib.vectorutils.get_vec_lyrs_lst(acc_pt_file)[0]
        processed_vals = rsgislib.vectorattrs.read_vec_column(acc_pt_file, vec_lyr, "Processed")
        #print(processed_vals)
        if 1 in processed_vals:
            vecs_dict.append({"file": acc_pt_file, "layer": vec_lyr})
        
    
#print(vecs_dict)
rsgislib.vectorutils.merge_vector_layers(vecs_dict, out_vec_file="gmw_v25_acc_pts.geojson", out_vec_lyr="gmw_v25_acc_pts", out_format="GeoJSON")
