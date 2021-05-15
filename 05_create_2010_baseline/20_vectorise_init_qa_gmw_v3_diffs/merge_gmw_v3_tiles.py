import glob
import rsgislib.vectorutils

out_file = "/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_init_v3_baseline_up2.gpkg"

input_vecs = glob.glob("/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_diff_v2_v3_further_qa_vec/*_gmw_add_v2_v3.gpkg")
rsgislib.vectorutils.mergeVectors2GPKG(input_vecs, out_file, "gmw_init_v3_add", True)

input_vecs = glob.glob("/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_diff_v2_v3_further_qa_vec/*_gmw_rmv_v2_v3.gpkg")
rsgislib.vectorutils.mergeVectors2GPKG(input_vecs, out_file, "gmw_init_v3_rmv", True)
