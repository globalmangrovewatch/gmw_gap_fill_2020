import glob
import os
import tqdm
import rsgislib.imageutils
import rsgislib.classification
import shutil

def msk_to_finite_values(input_h5, output_h5, datatype=None, lower_limit=None, upper_limit=None):
    import h5py
    import numpy

    rsgis_utils = rsgislib.RSGISPyUtils()
    if datatype is None:
        datatype = rsgislib.TYPE_32FLOAT
    h5_dtype = rsgis_utils.getNumpyCharCodesDataType(datatype)

    fH5 = h5py.File(input_h5, 'r')
    data_shp = fH5['DATA/DATA'].shape
    num_vars = data_shp[1]
    data = numpy.array(fH5['DATA/DATA'])
    data = data[numpy.isfinite(data).all(axis=1)]
    if lower_limit is not None:
        data = data[numpy.any(data > lower_limit, axis=1)]
    if upper_limit is not None:
        data = data[numpy.any(data < upper_limit, axis=1)]

    fH5Out = h5py.File(output_h5, 'w')
    dataGrp = fH5Out.create_group("DATA")
    metaGrp = fH5Out.create_group("META-DATA")
    dataGrp.create_dataset('DATA', data=data, chunks=(1000, num_vars), compression="gzip",
                           shuffle=True, dtype=h5_dtype)
    describDS = metaGrp.create_dataset("DESCRIPTION", (1,), dtype="S10")
    describDS[0] = 'finite values'.encode()
    fH5Out.close()


mng_files = glob.glob('/scratch/a.pfb/gmw_v2_gapfill/data/scn_h5_samples/*mng_smpls.h5')
oth_files = glob.glob('/scratch/a.pfb/gmw_v2_gapfill/data/scn_h5_samples/*oth_smpls.h5')

mng_all_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/mng_smpls_all.h5'
oth_all_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/oth_smpls_all.h5'

rsgislib.imageutils.mergeExtractedHDF5Data(mng_files, mng_all_samples)
rsgislib.imageutils.mergeExtractedHDF5Data(oth_files, oth_all_samples)


mng_all_mskd_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/mng_smpls_all_mskd.h5'
oth_all_mskd_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/oth_smpls_all_mskd.h5'

msk_to_finite_values(mng_all_samples, mng_all_mskd_samples, datatype=rsgislib.TYPE_16UINT, lower_limit=0, upper_limit=1000)
msk_to_finite_values(oth_all_samples, oth_all_mskd_samples, datatype=rsgislib.TYPE_16UINT, lower_limit=0, upper_limit=1000)


samples_dir = '/scratch/a.pfb/gmw_v2_gapfill/data/set_samples_h5'
n_samples = 1000
n_test_smpls = 250
n_valid_smpls = 250
n_train_smpls = 500
n_sets = 100
n_opt_smpl = 1 # 20% sample used for optimisation.

for i in tqdm.tqdm(range(n_sets)):
    mng_smps_file = os.path.join(samples_dir, "mng_samples_{}.h5".format(i+1))
    rsgislib.imageutils.randomSampleHDF5File(mng_all_mskd_samples, mng_smps_file, n_samples, i)

    mng_train_smps_file = os.path.join(samples_dir, "mng_train_samples_{}.h5".format(i + 1))
    mng_valid_smps_file = os.path.join(samples_dir, "mng_valid_samples_{}.h5".format(i + 1))
    mng_test_smps_file = os.path.join(samples_dir, "mng_test_samples_{}.h5".format(i + 1))
    rsgislib.classification.split_sample_train_valid_test(mng_smps_file, mng_train_smps_file, mng_valid_smps_file,
                                                          mng_test_smps_file, n_test_smpls, n_valid_smpls,
                                                          n_train_smpls, rand_seed=42, datatype=rsgislib.TYPE_16UINT)

    mng_train_smps_opt_file = os.path.join(samples_dir, "mng_train_samples_{}_opt.h5".format(i + 1))
    if n_opt_smpl < 1:
        rsgislib.imageutils.randomSampleHDF5File(mng_train_smps_file, mng_train_smps_opt_file,
                                                 int(n_train_smpls * n_opt_smpl), 42)
    else:
        shutil.copyfile(mng_train_smps_file, mng_train_smps_opt_file)

    oth_smps_file = os.path.join(samples_dir, "oth_samples_{}.h5".format(i + 1))
    rsgislib.imageutils.randomSampleHDF5File(oth_all_mskd_samples, oth_smps_file, n_samples, i)

    oth_train_smps_file = os.path.join(samples_dir, "oth_train_samples_{}.h5".format(i + 1))
    oth_valid_smps_file = os.path.join(samples_dir, "oth_valid_samples_{}.h5".format(i + 1))
    oth_test_smps_file = os.path.join(samples_dir, "oth_test_samples_{}.h5".format(i + 1))
    rsgislib.classification.split_sample_train_valid_test(oth_smps_file, oth_train_smps_file, oth_valid_smps_file,
                                                          oth_test_smps_file, n_test_smpls, n_valid_smpls,
                                                          n_train_smpls, rand_seed=42, datatype=rsgislib.TYPE_16UINT)

    oth_train_smps_opt_file = os.path.join(samples_dir, "oth_train_samples_{}_opt.h5".format(i + 1))
    if n_opt_smpl < 1:
        rsgislib.imageutils.randomSampleHDF5File(oth_train_smps_file, oth_train_smps_opt_file,
                                                 int(n_train_smpls * n_opt_smpl), 42)
    else:
        shutil.copyfile(oth_train_smps_file, oth_train_smps_opt_file)
