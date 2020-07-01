import rsgislib
import rsgislib.vectorutils

vec_file = '../sen2_roi_granule_vec.geojson'

granules = rsgislib.vectorutils.readVecColumn(vec_file, 'sen2_roi_granule_vec', 'Name')
print(granules)

rsgis_utils = rsgislib.RSGISPyUtils()
rsgis_utils.writeList2File(granules, '../sen2_roi_granule_lst.txt')
