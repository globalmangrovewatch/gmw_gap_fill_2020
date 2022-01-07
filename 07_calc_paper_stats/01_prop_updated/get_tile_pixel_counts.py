import rsgislib.tools.utils
import rsgislib.tools.filetools

import glob
import pprint

tile_stats_files = glob.glob("tile_stats/*.json")
updated_tiles = rsgislib.tools.utils.read_text_file_to_list("updated_tiles.txt")

tile_stats = dict()
total_area = 0.0
updated_area = 0.0

for tile_stats_file in tile_stats_files:
    basename = rsgislib.tools.filetools.get_file_basename(tile_stats_file)
    tile_name = basename.split("_")[1]
    print(tile_name)
    
    tile_count_stats = rsgislib.tools.utils.read_json_to_dict(tile_stats_file)
    
    tile_area = 0.0
    for rgn in tile_count_stats:
        tile_area += tile_count_stats[rgn]["area"]
    
    tile_stats[tile_name] = tile_area
    total_area += tile_area
    
    if tile_name in updated_tiles:
        updated_area += tile_area

pprint.pprint(tile_stats)


print(f"Total Area: {total_area}")
print(f"Updated Area: {updated_area}")

prop_updated = (updated_area / total_area) * 100


print(f"Proportion Updated: {prop_updated}")
