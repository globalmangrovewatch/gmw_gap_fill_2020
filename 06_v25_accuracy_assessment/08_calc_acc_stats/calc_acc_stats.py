import os
import rsgislib.tools.filetools
import rsgislib.classification.classaccuracymetrics
import rsgislib.vectorutils

vec_files = ["/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/00_acc_pt_sets/gmw_acc_roi_1_cls_acc_pts/gmw_acc_roi_1_cls_acc_pts_1.geojson",
             "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/00_acc_pt_sets/gmw_acc_roi_1_cls_acc_pts/gmw_acc_roi_1_cls_acc_pts_2.geojson",
             "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/00_acc_pt_sets/gmw_acc_roi_1_cls_acc_pts/gmw_acc_roi_1_cls_acc_pts_3.geojson",
             "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/00_acc_pt_sets/gmw_acc_roi_1_cls_acc_pts/gmw_acc_roi_1_cls_acc_pts_4.geojson",
             "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/00_acc_pt_sets/gmw_acc_roi_1_cls_acc_pts/gmw_acc_roi_1_cls_acc_pts_5.geojson",
             "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/00_acc_pt_sets/gmw_acc_roi_1_cls_acc_pts/gmw_acc_roi_1_cls_acc_pts_6.geojson",
             "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/00_acc_pt_sets/gmw_acc_roi_1_cls_acc_pts/gmw_acc_roi_1_cls_acc_pts_7.geojson",
             "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/00_acc_pt_sets/gmw_acc_roi_1_cls_acc_pts/gmw_acc_roi_1_cls_acc_pts_8.geojson",
             "/Users/pete/Development/globalmangrovewatch/gmw_gap_fill_2020/06_v25_accuracy_assessment/00_acc_pt_sets/gmw_acc_roi_1_cls_acc_pts/gmw_acc_roi_1_cls_acc_pts_9.geojson"]

vec_lyrs = list()
for vec_file in vec_files:
    vec_lyrs.append(rsgislib.tools.filetools.get_file_basename(vec_file))
        
ref_col = "ref_cls"
cls_col = "gmw_v25_cls"


tmp_dir = "/Users/pete/Temp/gmw_v25_extent/tmp"
out_plot_file = "/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_stats/gmw_acc_roi_1_cls_acc_stats/gmw_acc_roi_1_cls_acc_pts_intervals.png"

conf_thres_met, conf_thres_met_idx, f1_scores, f1_scr_intervals_rgn = rsgislib.classification.classaccuracymetrics.calc_acc_ptonly_metrics_vecsamples_f1_conf_inter_sets(vec_files, vec_lyrs, ref_col, cls_col, tmp_dir, conf_inter=95, conf_thres=0.05, out_plot_file=out_plot_file, sample_frac = 0.2, bootstrap_n = 2000)


print("conf_thres_met: {}".format(conf_thres_met))
print("conf_thres_met_idx: {}".format(conf_thres_met_idx))
print("f1_scores: {}".format(f1_scores))
print("f1_scr_intervals_rgn: {}".format(f1_scr_intervals_rgn))



