import rsgislib.vectorutils
import rsgislib.vectorutils.createvectors

import osgeo.gdal as gdal
import osgeo.ogr as ogr
import os
import tqdm

gdal.UseExceptions()

"""
def create_bboxs_for_pts(vec_file:str, vec_lyr:str, bbox_width:float, bbox_height:float, out_vec_file:str, out_vec_lyr:str,  out_format:str = "GPKG", del_exist_vec:bool=False, epsg_code:int=None):
    

    if os.path.exists(out_vec_file):
        if del_exist_vec:
            rsgislib.vectorutils.delete_vector_file(out_vec_file)
        else:
            raise Exception(
                "The output vector file ({}) already exists, "
                "remove it and re-run.".format(out_vec_file)
            )
            
    h_width = bbox_width / 2.0
    h_height = bbox_height / 2.0
    
    vec_ds_obj = gdal.OpenEx(vec_file, gdal.OF_VECTOR)
    if vec_ds_obj is None:
        raise Exception("The input vector file could not be opened.")
    vec_lyr_obj = vec_ds_obj.GetLayer(vec_lyr)
    if vec_lyr_obj is None:
        raise Exception("The input vector layer could not be opened.")
    if epsg_code is None:
        epsg_code = rsgislib.vectorutils.get_proj_epsg_from_vec(vec_file, vec_lyr)
    
    n_feats = vec_lyr_obj.GetFeatureCount(True)
    pbar = tqdm.tqdm(total=n_feats)
    
    counter = 0
    in_feature = vec_lyr_obj.GetNextFeature()
    out_bboxs = list()
    while in_feature:
    
        geom = in_feature.GetGeometryRef()
        if geom is not None:
            pt_x = geom.GetX()#.GetGeometryRef(0)
            pt_y = geom.GetY()
            
            x_min = pt_x - h_width
            x_max = pt_x + h_width
            y_min = pt_y - h_height
            y_max = pt_y + h_height
            
            out_bboxs.append([x_min, x_max, y_min, y_max])
        
        in_feature = vec_lyr_obj.GetNextFeature()
        counter = counter + 1
        pbar.update(1)
    
    pbar.close()
    vec_ds_obj = None
    
    rsgislib.vectorutils.createvectors.create_poly_vec_bboxs(out_vec_file, out_vec_lyr, out_format, epsg_code, out_bboxs, atts=None, att_types=None, overwrite=False)
"""

rsgislib.vectorutils.createvectors.create_bboxs_for_pts("roi_centre_pts.geojson", "roi_centre_pts", 0.2, 0.2, "roi_centre_bboxs.geojson", "roi_centre_bboxs",  out_format="GeoJSON", del_exist_vec=True, epsg_code=None)

