from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import rsgislib
import rsgislib.classification
import rsgislib.classification.classxgboost

logger = logging.getLogger(__name__)



def optimise_xgboost_binary_classifer(out_params_file, cls1_train_file, cls1_valid_file, cls2_train_file,
                                         cls2_valid_file, nthread=2, scale_pos_weight=None, mdl_cls_obj=None):
    """
    A function which performs a bayesian optimisation of the hyper-parameters for a binary xgboost
    classifier. Class 1 is the class which you are interested in and Class 2 is the 'other class'.

    This function requires that xgboost and skopt modules to be installed.

    :param out_params_file: The output model parameters which have been optimised.
    :param cls1_train_file: Training samples HDF5 file for the primary class (i.e., the one being classified)
    :param cls1_valid_file: Validation samples HDF5 file for the primary class (i.e., the one being classified)
    :param cls1_test_file: Testing samples HDF5 file for the primary class (i.e., the one being classified)
    :param cls2_train_file: Training samples HDF5 file for the 'other' class
    :param cls2_valid_file: Validation samples HDF5 file for the 'other' class
    :param cls2_test_file: Testing samples HDF5 file for the 'other' class
    :param nthread: The number of threads to use for the training.
    :param scale_pos_weight: Optional, default is None. If None then a value will automatically be calculated.
                             Parameter used to balance imbalanced training data.
    :param mdl_cls_obj: XGBoost object to allow continue training with a new dataset.

    """
    import h5py
    import numpy
    import xgboost as xgb
    import gc
    import json

    from sklearn.metrics import roc_auc_score
    from sklearn.metrics import accuracy_score

    from skopt.space import Real, Integer
    from skopt import gp_minimize

    print("Reading Class 1 Training")
    f = h5py.File(cls1_train_file, 'r')
    num_cls1_train_rows = f['DATA/DATA'].shape[0]
    print("num_cls1_train_rows = {}".format(num_cls1_train_rows))
    train_cls1 = numpy.array(f['DATA/DATA'])
    train_cls1_lbl = numpy.ones(num_cls1_train_rows, dtype=int)

    print("Reading Class 1 Validation")
    f = h5py.File(cls1_valid_file, 'r')
    num_cls1_valid_rows = f['DATA/DATA'].shape[0]
    print("num_cls1_valid_rows = {}".format(num_cls1_valid_rows))
    valid_cls1 = numpy.array(f['DATA/DATA'])
    valid_cls1_lbl = numpy.ones(num_cls1_valid_rows, dtype=int)

    print("Reading Class 2 Training")
    f = h5py.File(cls2_train_file, 'r')
    num_cls2_train_rows = f['DATA/DATA'].shape[0]
    print("num_cls2_train_rows = {}".format(num_cls2_train_rows))
    train_cls2 = numpy.array(f['DATA/DATA'])
    train_cls2_lbl = numpy.zeros(num_cls2_train_rows, dtype=int)

    print("Reading Class 2 Validation")
    f = h5py.File(cls2_valid_file, 'r')
    num_cls2_valid_rows = f['DATA/DATA'].shape[0]
    print("num_cls2_valid_rows = {}".format(num_cls2_valid_rows))
    valid_cls2 = numpy.array(f['DATA/DATA'])
    valid_cls2_lbl = numpy.zeros(num_cls2_valid_rows, dtype=int)

    print("Finished Reading Data")

    vaild_np = numpy.concatenate((valid_cls2, valid_cls1))
    vaild_lbl_np = numpy.concatenate((valid_cls2_lbl, valid_cls1_lbl))
    d_valid = xgb.DMatrix(vaild_np, label=vaild_lbl_np)

    d_train = xgb.DMatrix(numpy.concatenate((train_cls2, train_cls1)),
                          label=numpy.concatenate((train_cls2_lbl, train_cls1_lbl)))

    space = [Real(0.01, 0.9, name='eta'),
             Integer(0, 100, name='gamma'),
             Integer(2, 20, name='max_depth'),
             Integer(1, 10, name='min_child_weight'),
             Integer(0, 10, name='max_delta_step'),
             Real(0.5, 1, name='subsample'),
             Integer(2, 100, name='num_boost_round')
             ]

    if scale_pos_weight is None:
        scale_pos_weight = num_cls2_train_rows / num_cls1_train_rows
        if scale_pos_weight < 1:
            scale_pos_weight = 1
    print("scale_pos_weight = {}".format(scale_pos_weight))

    def _objective(values):
        params = {'eta'             : values[0],
                  'gamma'           : values[1],
                  'max_depth'       : values[2],
                  'min_child_weight': values[3],
                  'max_delta_step'  : values[4],
                  'subsample'       : values[5],
                  'nthread'         : nthread,
                  'eval_metric'     : 'auc',
                  'objective'       : 'binary:logistic'}

        print('\nNext set of params.....', params)

        num_boost_round = values[6]
        print("num_boost_round = {}.".format(num_boost_round))

        watchlist = [(d_train, 'train'), (d_valid, 'validation')]
        evals_results = {}
        model_xgb = xgb.train(params, d_train, num_boost_round, evals=watchlist, evals_result=evals_results,
                              verbose_eval=False, xgb_model=mdl_cls_obj)

        auc = -roc_auc_score(vaild_lbl_np, model_xgb.predict(d_valid))
        print('\nAUROC.....', -auc, ".....iter.....")
        gc.collect()
        return auc

    res_gp = gp_minimize(_objective, space, n_calls=20, random_state=0, n_random_starts=10)

    print("Best score={}".format(res_gp.fun))

    best_params = res_gp.x

    params = {'eta'             : float(best_params[0]),
              'gamma'           : int(best_params[1]),
              'max_depth'       : int(best_params[2]),
              'min_child_weight': int(best_params[3]),
              'max_delta_step'  : int(best_params[4]),
              'subsample'       : float(best_params[5]),
              'nthread'         : int(nthread),
              'eval_metric'     : 'auc',
              'objective'       : 'binary:logistic',
              'num_boost_round' : int(best_params[6])}

    with open(out_params_file, 'w') as fp:
        json.dump(params, fp, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)



class OptmiseXGBParams(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='opt_xgb_cls_params.py', descript=None)

    def do_processing(self, **kwargs):
        
        rsgislib.classification.classxgboost.optimise_xgboost_binary_classifer(self.params['out_cls_file'],
                                                                               self.params['mng_train_smps_file'],
                                                                               self.params['mng_valid_smps_file'],
                                                                               self.params['oth_train_smps_file'],
                                                                               self.params['oth_valid_smps_file'],
                                                                               nthread=1, scale_pos_weight=None,
                                                                               mdl_cls_obj=None)
        """
        optimise_xgboost_binary_classifer(self.params['out_cls_file'],
                                          self.params['mng_train_smps_file'],
                                          self.params['mng_valid_smps_file'],
                                          self.params['oth_train_smps_file'],
                                          self.params['oth_valid_smps_file'],
                                          nthread=1, scale_pos_weight=None,
                                          mdl_cls_obj=None)
        """

    def required_fields(self, **kwargs):
        return ["mng_train_smps_file", "mng_valid_smps_file", "oth_train_smps_file",
                "oth_valid_smps_file", "out_cls_file"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_cls_file']] = 'file'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Remove the output file.
        if os.path.exists(self.params['out_cls_file']):
            os.remove(self.params['out_cls_file'])

if __name__ == "__main__":
    OptmiseXGBParams().std_run()


