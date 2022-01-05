import glob
import pprint

import numpy
import scipy.stats

import rsgislib.tools.utils

param_files = glob.glob("cls_param_files/*.json")

params = ["eta", "gamma", "max_delta_step", "max_depth", "min_child_weight", "num_boost_round", "subsample"]

params_dict = {}
sum_params_dict = {}

for param in params:
    params_dict[param] = []
    sum_params_dict[param] = {"min": 0.0, "lqrt": 0.0, "mean": 0.0, "median": 0.0, "mqrt":0.0, "uqrt": 0.0, "max": 0.0, "mode":0.0}

for param_file in param_files:
    cls_params = rsgislib.tools.utils.read_json_to_dict(param_file)
    for param in params:
        params_dict[param].append(cls_params[param])

for param in params:
    #print(params_dict[param])
    param_arr = numpy.array(params_dict[param])
    sum_params_dict[param]["min"] = float(numpy.min(param_arr))
    sum_params_dict[param]["max"] = float(numpy.max(param_arr))
    sum_params_dict[param]["mean"] = float(numpy.mean(param_arr))
    sum_params_dict[param]["median"] = float(numpy.median(param_arr))
    sum_params_dict[param]["lqrt"] = float(numpy.percentile(param_arr, 25))
    sum_params_dict[param]["mqrt"] = float(numpy.percentile(param_arr, 50))
    sum_params_dict[param]["uqrt"] = float(numpy.percentile(param_arr, 75))
    mode_rslt = scipy.stats.mode(param_arr)
    sum_params_dict[param]["mode"] = float(mode_rslt.mode[0])


pprint.pprint(sum_params_dict)
rsgislib.tools.utils.write_dict_to_json(sum_params_dict, "cls_params_sum.json")

