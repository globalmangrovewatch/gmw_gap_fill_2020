
def spatial_select(vec_file, vec_lyr, vec_roi_file, vec_roi_lyr, out_vec_file, out_vec_lyr=None, out_format="GeoJSON"):
    import geopandas
    import numpy
    import tqdm
    base_gpdf = geopandas.read_file(vec_file, layer=vec_lyr)
    roi_gpdf = geopandas.read_file(vec_roi_file, layer=vec_roi_lyr)    
    base_gpdf['msk'] = numpy.zeros((base_gpdf.shape[0]), dtype=bool)    
    geoms = list()
    for i in tqdm.tqdm(range(roi_gpdf.shape[0])):
        inter = base_gpdf['geometry'].intersects(roi_gpdf.iloc[i]['geometry'])
        base_gpdf.loc[inter, 'msk'] = True
    base_gpdf = base_gpdf[base_gpdf['msk']]
    base_gpdf = base_gpdf.drop(['msk'], axis=1)
    if base_gpdf.shape[0] > 0:
        if out_format == 'GPKG':
            base_gpdf.to_file(out_vec_file, layer=out_vec_lyr, driver=out_format)
        else:
            base_gpdf.to_file(out_vec_file, driver=out_format)
    else:
        raise Exception("No output file as no features intersect.")
    
vec_file = 'sen2_granule_vec.geojson'
vec_lyr = 'sen2_granule_vec'
vec_roi_file = '../../00_define_rois/gmw_missing_regions.geojson'
vec_roi_lyr = 'gmw_missing_regions'
out_vec_file = '../sen2_roi_granule_vec.geojson'
spatial_select(vec_file, vec_lyr, vec_roi_file, vec_roi_lyr, out_vec_file, out_vec_lyr=None, out_format="GeoJSON")
