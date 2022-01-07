import pprint

import rsgislib.vectorutils
import rsgislib.tools.utils

gmw_deg_grid_vec_file = "/Users/pete/Dropbox/University/Research/Data/Mangroves/GMWDegreeTilesExtended.gpkg"
gmw_deg_grid_vec_lyr = "GMWDegreeTiles"


up_v25_regs_vec_file = "/Users/pete/Dropbox/University/Research/Papers/202111_bunting_etal_gmw_25_extent/Figures/roi_world_map/sen2_roi_granule_vec.geojson"
up_v25_regs_vec_lyr = "sen2_roi_granule_vec"


out_vals_list = rsgislib.vectorutils.get_att_lst_select_feats(gmw_deg_grid_vec_file, gmw_deg_grid_vec_lyr, ["tile_name", "gmw_name"], up_v25_regs_vec_file, up_v25_regs_vec_lyr)

tile_names = list()
for out_vals in out_vals_list:
    tile_names.append(out_vals["tile_name"])


tile_names_set = set(tile_names)

pprint.pprint(tile_names_set)

rsgislib.tools.utils.write_list_to_file(tile_names_set, "updated_tiles.txt")
