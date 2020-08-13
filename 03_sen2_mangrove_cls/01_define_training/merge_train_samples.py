import rsgislib.vectorutils
import glob

input_files = glob.glob("/scratch/a.pfb/gmw_v2_gapfill/data/granule_mang_train_vecs/*.gpkg")
output_file = "/scratch/a.pfb/gmw_v2_gapfill/data/granule_mang_train_smpls.gpkg"
rsgislib.vectorutils.merge_vector_files(input_files, output_file, output_lyr="samples", out_format='GPKG', out_epsg=4326)
