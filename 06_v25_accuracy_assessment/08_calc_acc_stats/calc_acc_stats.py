import os
import rsgislib.tools.filetools
import rsgislib.classification.classaccuracymetrics
import rsgislib.vectorutils


vec_files = ["/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_pts_sets/gmw_acc_roi_12_cls_acc_pts/gmw_acc_roi_12_cls_acc_pts_1.geojson",
             "/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_pts_sets/gmw_acc_roi_12_cls_acc_pts/gmw_acc_roi_12_cls_acc_pts_2.geojson",
             "/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_pts_sets/gmw_acc_roi_12_cls_acc_pts/gmw_acc_roi_12_cls_acc_pts_3.geojson",
             "/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_pts_sets/gmw_acc_roi_12_cls_acc_pts/gmw_acc_roi_12_cls_acc_pts_4.geojson"]
             
ref_col = "ref_cls"
cls_col = "gmw_v25_cls"
out_dir = "/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_stats/gmw_acc_roi_12_cls_acc_stats"
"""
for vec_file in vec_files:
    vec_lyr = rsgislib.tools.filetools.get_file_basename(vec_file)
    
    out_json_file = os.path.join(out_dir, "{}_stat.json".format(vec_lyr))
    out_csv_file = os.path.join(out_dir, "{}_stat.csv".format(vec_lyr))

    rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples(vec_file, vec_lyr, ref_col, cls_col, out_json_file=out_json_file, out_csv_file=out_csv_file)
"""


merged_acc_pts_file = "/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_stats/gmw_acc_roi_12_cls_acc_stats/gmw_acc_roi_12_cls_acc_merged_pts.gpkg"
merged_acc_pts_lyr = "cls_acc_pts"
#rsgislib.vectorutils.merge_vector_files(vec_files, merged_acc_pts_file, out_vec_lyr=merged_acc_pts_lyr, out_format="GPKG")

out_json_file = os.path.join(out_dir, "{}_merged_stat.json".format(merged_acc_pts_lyr))
out_csv_file = os.path.join(out_dir, "{}_merged_stat.csv".format(merged_acc_pts_lyr))
#rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples(merged_acc_pts_file, merged_acc_pts_lyr, ref_col, cls_col, out_json_file=out_json_file, out_csv_file=out_csv_file)

rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples_bootstrap_uncertainty(merged_acc_pts_file, merged_acc_pts_lyr, ref_col, cls_col, out_json_file=None, out_csv_file=None, sample_frac=0.2, bootstrap_n=1000)



vec_file = "/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_pts_sets/gmw_acc_roi_12_cls_acc_pts/gmw_acc_roi_12_cls_acc_pts_1.geojson"
vec_lyr = "gmw_acc_roi_12_cls_acc_pts_1"
rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples_bootstrap_uncertainty(vec_file, vec_lyr, ref_col, cls_col, out_json_file=None, out_csv_file=None, sample_frac=0.2, bootstrap_n=1000)

