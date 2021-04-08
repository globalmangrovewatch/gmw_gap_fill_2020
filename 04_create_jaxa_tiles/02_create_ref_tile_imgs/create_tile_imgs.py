
import rsgislib
import rsgislib.imageutils
import geopandas
import tqdm
import os

rsgis_utils = rsgislib.RSGISPyUtils()
wkt_str = rsgis_utils.getWKTFromEPSGCode(4326)

jaxa_ref_tiles_file = 'GMWDegreeTilesExtended.gpkg'
jaxa_ref_tiles_lyr = 'GMWDegreeTiles'

tiles_gpdf = geopandas.read_file(jaxa_ref_tiles_file, layer=jaxa_ref_tiles_lyr)

out_img_res = 0.000222222222222
out_img_width = 4500
out_img_height = 4500

out_dir = '/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_ref_tiles'

n_imgs = tiles_gpdf.shape[0]
for i in range(n_imgs):
    x_min_val = tiles_gpdf.loc[i]['MinX']
    x_max_val = tiles_gpdf.loc[i]['MaxX']
    y_min_val = tiles_gpdf.loc[i]['MinY']
    y_max_val = tiles_gpdf.loc[i]['MaxY']
    gmw_tile_name = tiles_gpdf.loc[i]['gmw_name']

    out_file = os.path.join(out_dir, '{}_tile.kea'.format(gmw_tile_name))

    print("{} of {}: {}".format(i, n_imgs, out_file))

    rsgislib.imageutils.createBlankImage(out_file, 1, out_img_width, out_img_height, x_min_val, y_max_val, out_img_res, 0, '', wkt_str, 'KEA', rsgislib.TYPE_8UINT)







