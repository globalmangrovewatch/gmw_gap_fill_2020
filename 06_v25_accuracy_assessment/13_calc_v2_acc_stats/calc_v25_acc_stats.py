import os
import rsgislib.vectorutils
import rsgislib.classification.classaccuracymetrics


def calc_acc_stats(vec_file, vec_lyr, ref_col, cls_col, acc_stats_json_file, acc_stats_csv_file, acc_conf_stats_json_file):
    rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples(vec_file, vec_lyr, ref_col, cls_col, out_json_file=acc_stats_json_file, out_csv_file=acc_stats_csv_file)
    rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples_bootstrap_conf_interval(vec_file, vec_lyr, ref_col, cls_col, out_json_file=acc_conf_stats_json_file, sample_n_smps=1000, bootstrap_n=2000)



v2_comp_site_idxs = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 27, 42, 48, 59]

print(len(v2_comp_site_idxs))

all_sites = list()
all_sites_json = list()
v2_comp_sites = list()
v2_comp_sites_json = list()

ref_col = "ref_cls"
cls_col = "gmw_v25_cls"

out_set_dir = "set_v25_accs"
acc_vec_dir = "../10_calc_set_acc_stats/set_acc_stats/"
for i in range(60):
    vec_file = os.path.join(acc_vec_dir, "gmw_v25_set_{}_acc_pts.geojson".format(i+1))
    if os.path.exists(vec_file):
        vec_lyr = rsgislib.vectorutils.get_vec_lyrs_lst(vec_file)[0]
        
        acc_stats_json_file = os.path.join(out_set_dir, "gmw_v25_set_{}_acc_stats.json".format(i+1))
        acc_stats_csv_file = os.path.join(out_set_dir, "gmw_v25_set_{}_acc_stats.csv".format(i+1))
        acc_conf_stats_json_file = os.path.join(out_set_dir, "gmw_v25_set_{}_acc_conf_stats.json".format(i+1))
        
        if (i+1) in v2_comp_site_idxs:
            v2_comp_sites.append({"file": vec_file, "layer": vec_lyr})
            v2_comp_sites_json.append(acc_stats_json_file)
        all_sites.append({"file": vec_file, "layer": vec_lyr})
        all_sites_json.append(acc_stats_json_file)
        
        calc_acc_stats(vec_file, vec_lyr, ref_col, cls_col, acc_stats_json_file, acc_stats_csv_file, acc_conf_stats_json_file)


vec_acc_pts_file = "gmw_v25_all_acc_pts.geojson"
vec_acc_pts_lyr = "gmw_v25_all_acc_pts"
rsgislib.vectorutils.merge_vector_layers(all_sites, out_vec_file=vec_acc_pts_file, out_vec_lyr=vec_acc_pts_lyr, out_format="GeoJSON")

acc_stats_json_file = "gmw_v25_all_acc_stats.json"
acc_stats_csv_file = "gmw_v25_all_acc_stats.csv"
acc_conf_stats_json_file = "gmw_v25_set_all_acc_conf_stats.json"

calc_acc_stats(vec_acc_pts_file, vec_acc_pts_lyr, ref_col, cls_col, acc_stats_json_file, acc_stats_csv_file, acc_conf_stats_json_file)

out_acc_json_sum_file = "gmw_v25_all_acc_stats_set_sum.json"
rsgislib.classification.classaccuracymetrics.summarise_multi_acc_ptonly_metrics(all_sites_json, out_acc_json_sum_file)

vec_acc_pts_file = "gmw_v25_v2_comp_acc_pts.geojson"
vec_acc_pts_lyr = "gmw_v25_v2_comp_acc_pts"
rsgislib.vectorutils.merge_vector_layers(v2_comp_sites, out_vec_file=vec_acc_pts_file, out_vec_lyr=vec_acc_pts_lyr, out_format="GeoJSON")

acc_stats_json_file = "gmw_v25_comp_acc_stats.json"
acc_stats_csv_file = "gmw_v25_comp_acc_stats.csv"
acc_conf_stats_json_file = "gmw_v25_comp_acc_conf_stats.json"

calc_acc_stats(vec_acc_pts_file, vec_acc_pts_lyr, ref_col, cls_col, acc_stats_json_file, acc_stats_csv_file, acc_conf_stats_json_file)

out_acc_json_sum_file = "gmw_v25_comp_acc_stats_set_sum.json"
rsgislib.classification.classaccuracymetrics.summarise_multi_acc_ptonly_metrics(v2_comp_sites_json, out_acc_json_sum_file)