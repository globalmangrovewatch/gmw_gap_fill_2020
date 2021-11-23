import glob
import rsgislib.classification.classaccuracymetrics


acc_json_all_files = glob.glob("set_acc_stats/gmw_v25_set_*_acc_stats.json")
acc_json_files = list()
for acc_json_file in acc_json_all_files:
    if 'conf' not in acc_json_file:
        acc_json_files.append(acc_json_file)
print(acc_json_files)

out_acc_json_sum_file = "gmw_v25_set_sum_acc_stats.json"
rsgislib.classification.classaccuracymetrics.summarise_multi_acc_ptonly_metrics(acc_json_files, out_acc_json_sum_file)

