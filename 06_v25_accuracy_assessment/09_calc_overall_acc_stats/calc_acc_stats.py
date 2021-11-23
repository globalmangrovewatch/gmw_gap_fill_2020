import rsgislib.classification.classaccuracymetrics

vec_file="gmw_v25_acc_pts.geojson"
vec_lyr="gmw_v25_acc_pts"

ref_col = "ref_cls"
cls_col = "gmw_v25_cls"

acc_stats_json_file = "gmw_v25_overall_acc_stats.json"
acc_stats_csv_file = "gmw_v25_overall_acc_stats.csv"
acc_conf_stats_json_file = "gmw_v25_overall_conf_acc_stats.json"

rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples(vec_file, vec_lyr, ref_col, cls_col, out_json_file=acc_stats_json_file, out_csv_file=acc_stats_csv_file)
rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples_bootstrap_conf_interval(vec_file, vec_lyr, ref_col, cls_col, out_json_file=acc_conf_stats_json_file, sample_n_smps=1000, bootstrap_n=2000)

