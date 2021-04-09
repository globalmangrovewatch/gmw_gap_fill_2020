import glob
import rsgislib.vectorutils

input_vecs = glob.glob("/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_init_v3_qa_vec/*.gpkg")

out_file = "/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_init_v3_baseline_up1.gpkg"

rsgislib.vectorutils.mergeVectors2GPKG(input_vecs, out_file, "gmw_init_v3", False)