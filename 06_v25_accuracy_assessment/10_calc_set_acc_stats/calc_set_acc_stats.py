import os
import glob

import rsgislib.vectorutils
import rsgislib.vectorattrs
import rsgislib.classification.classaccuracymetrics

out_dir = "set_acc_stats"
ref_col = "ref_cls"
cls_col = "gmw_v25_cls"

acc_pts_base_dir = "../00_acc_pt_sets"
for i in range(60):
    acc_pts_dir = os.path.join(acc_pts_base_dir, "gmw_acc_roi_{}_cls_acc_pts".format(i+1))
    acc_pt_files = glob.glob(os.path.join(acc_pts_dir, "*.geojson"))
    vecs_dict = list()
    for acc_pt_file in acc_pt_files:
        vec_lyr = rsgislib.vectorutils.get_vec_lyrs_lst(acc_pt_file)[0]
        processed_vals = rsgislib.vectorattrs.read_vec_column(acc_pt_file, vec_lyr, "Processed")
        #print(processed_vals)
        if 1 in processed_vals:
            vecs_dict.append({"file": acc_pt_file, "layer": vec_lyr})
            
    if len(vecs_dict) > 0:
        vec_acc_pts_file = "gmw_v25_tmp_set_acc_pts.geojson"
        vec_acc_pts_lyr = "gmw_v25_tmp_set_acc_pts"
        rsgislib.vectorutils.merge_vector_layers(vecs_dict, out_vec_file=vec_acc_pts_file, out_vec_lyr=vec_acc_pts_lyr, out_format="GeoJSON")
        
        acc_stats_json_file = os.path.join(out_dir, "gmw_v25_set_{}_acc_stats.json".format(i+1))
        acc_stats_csv_file = os.path.join(out_dir, "gmw_v25_set_{}_acc_stats.csv".format(i+1))
        acc_conf_stats_json_file = os.path.join(out_dir, "gmw_v25_set_{}_conf_acc_stats.json".format(i+1))
        
        rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples(vec_acc_pts_file, vec_acc_pts_lyr, ref_col, cls_col, out_json_file=acc_stats_json_file, out_csv_file=acc_stats_csv_file)
        rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples_bootstrap_conf_interval(vec_acc_pts_file, vec_acc_pts_lyr, ref_col, cls_col, out_json_file=acc_conf_stats_json_file, sample_n_smps=1000, bootstrap_n=2000)
        
        os.remove(vec_acc_pts_file)
    
