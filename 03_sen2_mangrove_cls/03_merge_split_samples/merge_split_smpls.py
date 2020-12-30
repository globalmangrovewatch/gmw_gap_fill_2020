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
wat_files = glob.glob('/scratch/a.pfb/gmw_v2_gapfill/data/scn_h5_samples/*wat_smpls.h5')

mng_all_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/mng_smpls_all.h5'
oth_all_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/oth_smpls_all.h5'
wat_all_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/wat_smpls_all.h5'

#rsgislib.imageutils.mergeExtractedHDF5Data(mng_files, mng_all_samples)
#rsgislib.imageutils.mergeExtractedHDF5Data(oth_files, oth_all_samples)
#rsgislib.imageutils.mergeExtractedHDF5Data(wat_files, wat_all_samples)

mng_all_mskd_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/mng_smpls_all_mskd.h5'
oth_all_mskd_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/oth_smpls_all_mskd.h5'
wat_all_mskd_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/wat_smpls_all_mskd.h5'

#msk_to_finite_values(mng_all_samples, mng_all_mskd_samples, datatype=rsgislib.TYPE_16UINT, lower_limit=0, upper_limit=1000)
#msk_to_finite_values(oth_all_samples, oth_all_mskd_samples, datatype=rsgislib.TYPE_16UINT, lower_limit=0, upper_limit=1000)
#msk_to_finite_values(wat_all_samples, wat_all_mskd_samples, datatype=rsgislib.TYPE_16UINT, lower_limit=0, upper_limit=1000)

print(rsgislib.classification.get_num_samples(mng_all_mskd_samples))
print(rsgislib.classification.get_num_samples(oth_all_mskd_samples))
print(rsgislib.classification.get_num_samples(wat_all_mskd_samples))


wat_train_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/wat_smpls_train.h5'
wat_test_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/wat_smpls_test.h5'
wat_valid_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/wat_smpls_valid.h5'
#rsgislib.classification.split_sample_train_valid_test(wat_all_mskd_samples, wat_train_samples, wat_test_samples,
#                                                      wat_valid_samples, 100, 20000, 50000,
#                                                      rand_seed=42, datatype=rsgislib.TYPE_16UINT)

mng_train_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/mng_smpls_train.h5'
mng_test_samples = '/scratch/a.pfb/gmw_v2_gapfill/data/mng_smpls_test.h5'

#rsgislib.imageutils.randomSampleHDF5File(mng_all_mskd_samples, mng_train_samples, 50000, 42)
#rsgislib.imageutils.randomSampleHDF5File(mng_all_mskd_samples, mng_test_samples, 20000, 84)

print(rsgislib.classification.get_num_samples(wat_train_samples))
print(rsgislib.classification.get_num_samples(wat_test_samples))

print(rsgislib.classification.get_num_samples(mng_train_samples))
print(rsgislib.classification.get_num_samples(mng_test_samples))


samples_dir = '/scratch/a.pfb/gmw_v2_gapfill/data/set_samples_h5_v2'
n_samples = 200000
n_test_smpls = 50000
n_valid_smpls = 50000
n_train_smpls = 100000
n_sets = 100
n_opt_smpl = 0.2 # 20% sample used for optimisation.

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

    mng_train_smps_xtr_file = os.path.join(samples_dir, "mng_train_samples_xtr_{}.h5".format(i + 1))
    rsgislib.imageutils.mergeExtractedHDF5Data([mng_train_smps_file, mng_train_samples], mng_train_smps_xtr_file)

    mng_test_smps_xtr_file = os.path.join(samples_dir, "mng_test_samples_xtr_{}.h5".format(i + 1))
    rsgislib.imageutils.mergeExtractedHDF5Data([mng_test_smps_file, mng_test_samples], mng_test_smps_xtr_file)

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
        rsgislib.imageutils.randomSampleHDF5File(oth_train_smps_file, oth_train_smps_opt_file, int(n_train_smpls * n_opt_smpl), 42)
    else:
        shutil.copyfile(oth_train_smps_file, oth_train_smps_opt_file)

    oth_train_smps_xtr_file = os.path.join(samples_dir, "oth_train_samples_xtr_{}.h5".format(i + 1))
    rsgislib.imageutils.mergeExtractedHDF5Data([oth_train_smps_file, wat_train_samples], oth_train_smps_xtr_file)

    oth_test_smps_xtr_file = os.path.join(samples_dir, "oth_test_samples_xtr_{}.h5".format(i + 1))
    rsgislib.imageutils.mergeExtractedHDF5Data([oth_test_smps_file, wat_test_samples], oth_test_smps_xtr_file)

