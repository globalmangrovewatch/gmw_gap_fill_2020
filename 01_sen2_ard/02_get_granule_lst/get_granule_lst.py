import rsgislib
import rsgislib.vectorutils

vec_file = '../sen2_roi_granule_vec.geojson'

granules = rsgislib.vectorutils.readVecColumn(vec_file, 'sen2_roi_granule_vec', 'Name')
print(granules)

