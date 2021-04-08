import glob
import rsgislib.vectorutils

out_file = "/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_init_v3_diffs.gpkg"

input_vecs = glob.glob("/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_diff_v2_v3_vec/*_gmw_add_v2_v3.gpkg")
rsgislib.vectorutils.mergeVectors2GPKG(input_vecs, out_file, "gmw_init_v3_add", False)

input_vecs = glob.glob("/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_diff_v2_v3_vec/*_gmw_rmv_v2_v3.gpkg")
rsgislib.vectorutils.mergeVectors2GPKG(input_vecs, out_file, "gmw_init_v3_rmv", True)
