import glob
import rsgislib.imageutils

mng_files = glob.glob('/scratch/a.pfb/gmw_v2_gapfill/data/scn_h5_samples/*mng_smpls.h5')
oth_files = glob.glob('/scratch/a.pfb/gmw_v2_gapfill/data/scn_h5_samples/*oth_smpls.h5')

rsgislib.imageutils.mergeExtractedHDF5Data(mng_files, '/scratch/a.pfb/gmw_v2_gapfill/data/mng_smpls_all.h5')
rsgislib.imageutils.mergeExtractedHDF5Data(oth_files, '/scratch/a.pfb/gmw_v2_gapfill/data/oth_smpls_all.h5')




