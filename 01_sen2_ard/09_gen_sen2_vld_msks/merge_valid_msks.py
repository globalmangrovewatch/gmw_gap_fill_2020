import glob
import rsgislib.vectorutils

input_vecs = glob.glob("/scratch/a.pfb/gmw_v2_gapfill/data/granule_vld_msks/*.gpkg")

rsgislib.vectorutils.mergeVectors2GPKG(input_vecs, '/scratch/a.pfb/gmw_v2_gapfill/data/granule_vld_msks.gpkg', 'granule_vld_msks', False)

