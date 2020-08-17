import glob
import os
import tqdm
import rsgislib.imageutils
import rsgislib.classification

mng_files = glob.glob('/scratch/a.pfb/gmw_v2_gapfill/data/scn_h5_samples/*mng_smpls.h5')
oth_files = glob.glob('/scratch/a.pfb/gmw_v2_gapfill/data/scn_h5_samples/*oth_smpls.h5')

mng_all_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/mng_smpls_all.h5'
oth_all_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/oth_smpls_all.h5'

rsgislib.imageutils.mergeExtractedHDF5Data(mng_files, mng_all_samples)
rsgislib.imageutils.mergeExtractedHDF5Data(oth_files, oth_all_samples)

samples_dir = '/scratch/a.pfb/gmw_v2_gapfill/data/set_samples_h5'
n_samples = 1000
n_test_smpls = 250
n_valid_smpls = 250
n_train_smpls = 500
n_sets = 100

for i in tqdm.tqdm(range(n_sets)):
    mng_smps_file = os.path.join(samples_dir, "mng_samples_{}.h5".format(i+1))
    rsgislib.imageutils.randomSampleHDF5File(mng_all_samples, mng_smps_file, n_samples, i)

    mng_train_smps_file = os.path.join(samples_dir, "mng_train_samples_{}.h5".format(i + 1))
    mng_test_smps_file = os.path.join(samples_dir, "mng_test_samples_{}.h5".format(i + 1))
    mng_valid_smps_file = os.path.join(samples_dir, "mng_valid_samples_{}.h5".format(i + 1))
    rsgislib.classification.split_sample_train_valid_test(mng_smps_file, mng_train_smps_file, mng_valid_smps_file,
                                                          mng_test_smps_file, n_test_smpls, n_valid_smpls,
                                                          n_train_smpls, rand_seed=42, datatype=rsgislib.TYPE_16UINT)

    oth_smps_file = os.path.join(samples_dir, "oth_samples_{}.h5".format(i + 1))
    rsgislib.imageutils.randomSampleHDF5File(oth_all_samples, oth_smps_file, n_samples, i)

    oth_train_smps_file = os.path.join(samples_dir, "oth_train_samples_{}.h5".format(i + 1))
    oth_test_smps_file = os.path.join(samples_dir, "oth_test_samples_{}.h5".format(i + 1))
    oth_valid_smps_file = os.path.join(samples_dir, "oth_valid_samples_{}.h5".format(i + 1))
    rsgislib.classification.split_sample_train_valid_test(oth_smps_file, oth_train_smps_file, oth_valid_smps_file,
                                                          oth_test_smps_file, n_test_smpls, n_valid_smpls,
                                                          n_train_smpls, rand_seed=42, datatype=rsgislib.TYPE_16UINT)

