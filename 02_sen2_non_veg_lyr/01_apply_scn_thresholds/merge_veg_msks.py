import glob
import rsgislib.vectorutils

def vec_within_vec(vec_base_file, vec_base_lyr, vec_comp_file, vec_comp_lyr):
    """
    Function to test whether the comparison vector layer within with the
    base vector layer.

    Note. This function iterates through the geometries of both files performing
    a comparison and therefore can be very slow to execute for large vector files.

    :param vec_base_file: vector layer file used as the base layer
    :param vec_base_lyr: vector layer used as the base layer
    :param vec_comp_file: vector layer file used as the comparison layer
    :param vec_comp_lyr: vector layer used as the comparison layer
    :return: boolean

    """
    import osgeo.gdal as gdal
    import tqdm

    gdal.UseExceptions()

    dsVecBaseObj = gdal.OpenEx(vec_base_file, gdal.OF_READONLY)
    if dsVecBaseObj is None:
        raise Exception("Could not open '{}'".format(vec_base_file))

    lyrVecBaseObj = dsVecBaseObj.GetLayerByName(vec_base_lyr)
    if lyrVecBaseObj is None:
        raise Exception("Could not find layer '{}'".format(vec_base_lyr))

    dsVecCompObj = gdal.OpenEx(vec_comp_file, gdal.OF_READONLY)
    if dsVecCompObj is None:
        raise Exception("Could not open '{}'".format(vec_comp_file))

    lyrVecCompObj = dsVecCompObj.GetLayerByName(vec_comp_lyr)
    if lyrVecCompObj is None:
        raise Exception("Could not find layer '{}'".format(vec_comp_lyr))

    n_feats = lyrVecCompObj.GetFeatureCount(True)
    pbar = tqdm.tqdm(total=n_feats)
    is_within = True

    lyrVecCompObj.ResetReading()
    comp_feat = lyrVecCompObj.GetNextFeature()
    while comp_feat is not None:
        comp_geom = comp_feat.GetGeometryRef()
        comp_feat_within = False
        if comp_geom is not None:
            lyrVecBaseObj.ResetReading()
            base_feat = lyrVecBaseObj.GetNextFeature()
            while base_feat is not None:
                base_geom = base_feat.GetGeometryRef()
                if base_geom is not None:
                    if comp_geom.Within(base_geom):
                        comp_feat_within = True
                        break
                base_feat = lyrVecBaseObj.GetNextFeature()

        if not comp_feat_within:
            is_within = False
            break
        pbar.update(1)
        comp_feat = lyrVecCompObj.GetNextFeature()

    dsVecBaseObj = None
    dsVecCompObj = None

    return is_within


def calc_bbox_area(bbox):
    # width x height
    return (bbox[1] - bbox[0]) * (bbox[3] - bbox[2])


def utm_from_epsg(epsg_code):
    """
Return UTM zone and hemisphere from a EPSG code using WGS84 datum.

:param epsg_code: epsg code for the UTM projection.
:return: zone, hemisphere

"""
    h_zone = epsg_code - 32000

    if h_zone < 700:
        hemisphere = 'N'
        zone = h_zone - 600
    else:
        hemisphere = 'S'
        zone = h_zone - 700

    return zone, hemisphere


def epsg_for_UTM(zone, hemisphere):
    """
Return EPSG code for given UTM zone and hemisphere using WGS84 datum.

:param zone: UTM zone
:param hemisphere: hemisphere either 'N' or 'S'
:return: corresponding EPSG code

"""
    if hemisphere not in ['N', 'S']:
        raise Exception('Invalid hemisphere ("N" or "S").')

    if zone < 0 or zone > 60:
        raise Exception('UTM zone ouside valid range.')

    if hemisphere == 'N':
        ns = 600
    else:
        ns = 700

    if zone == 0:
        zone = 61

    return int(32000 + ns + zone)


def zero_pad_num_str(num_val, str_len=3, round_num=False, round_n_digts=0, integerise=False):
    """
    A function which zero pads a number to make a string

    :param num_val: number value to be processed.
    :param str_len: the number of characters in the output string.
    :param round_num: boolean whether to round the input number value.
    :param round_n_digts: If rounding, the number of digits following decimal points to round to.
    :param integerise: boolean whether to integerise the input number
    :return: string with the padded numeric value.

    """
    if round_num:
        num_val = round(num_val, round_n_digts)
    if integerise:
        num_val = int(num_val)

    num_str = "{}".format(num_val)
    num_str = num_str.zfill(str_len)
    return num_str


def geopd_check_polys_wgs84bounds_geometry(data_gdf, width_thres=350):
    """
    A function which checks a polygons within the geometry of a geopanadas dataframe
    for specific case where they on the east/west edge (i.e., 180 / -180) and are therefore
    being wrapped around the world. For example, this function would change a longitude
    -179.91 to 180.01. The geopandas dataframe will be edit in place.

    This function will import the shapely library.

    :param data_gdf: geopandas dataframe.
    :param width_thres: The threshold (default 350 degrees) for the width of a polygon for which
                        the polygons will be checked, looping through all the coordinates
    :return: geopandas dataframe

    """
    from shapely.geometry import Polygon, LinearRing
    import geopandas

    out_gdf = geopandas.GeoDataFrame()
    out_gdf['geometry'] = None
    i_geom = 0

    for index, row in data_gdf.iterrows():
        n_east = 0
        n_west = 0
        row_bbox = row['geometry'].bounds
        row_width = row_bbox[2] - row_bbox[0]
        if row_width > width_thres:
            if row['geometry'].geom_type == 'Polygon':
                for coord in row['geometry'].exterior.coords:
                    if coord[0] < 0:
                        n_west += 1
                    else:
                        n_east += 1
                east_focus = True
                if n_west > n_east:
                    east_focus = False

                out_coords = []
                for coord in row['geometry'].exterior.coords:
                    out_coord = [coord[0], coord[1]]
                    if east_focus:
                        if coord[0] < 0:
                            diff = coord[0] - -180
                            out_coord[0] = 180 + diff
                    else:
                        if coord[0] > 0:
                            diff = 180 - coord[0]
                            out_coord[0] = -180 - diff
                    out_coords.append(out_coord)

                out_holes = []
                for hole in row['geometry'].interiors:
                    hole_coords = []
                    for coord in hole.coords:
                        out_coord = [coord[0], coord[1]]
                        if east_focus:
                            if coord[0] < 0:
                                diff = coord[0] - -180
                                out_coord[0] = 180 + diff
                        else:
                            if coord[0] > 0:
                                diff = 180 - coord[0]
                                out_coord[0] = -180 - diff
                        hole_coords.append(out_coord)
                    out_holes.append(LinearRing(hole_coords))
                out_gdf.loc[i_geom, 'geometry'] = Polygon(out_coords, holes=out_holes)
                i_geom += 1
                #polys.append(Polygon(out_coords, holes=out_holes))
            elif row['geometry'].geom_type == 'MultiPolygon':
                for poly in row['geometry']:
                    for coord in poly.exterior.coords:
                        if coord[0] < 0:
                            n_west += 1
                        else:
                            n_east += 1
                    east_focus = True
                    if n_west > n_east:
                        east_focus = False

                    out_coords = []
                    for coord in poly.exterior.coords:
                        out_coord = [coord[0], coord[1]]
                        if east_focus:
                            if coord[0] < 0:
                                diff = coord[0] - -180
                                out_coord[0] = 180 + diff
                        else:
                            if coord[0] > 0:
                                diff = 180 - coord[0]
                                out_coord[0] = -180 - diff
                        out_coords.append(out_coord)

                    out_holes = []
                    for hole in poly.interiors:
                        hole_coords = []
                        for coord in hole.coords:
                            out_coord = [coord[0], coord[1]]
                            if east_focus:
                                if coord[0] < 0:
                                    diff = coord[0] - -180
                                    out_coord[0] = 180 + diff
                            else:
                                if coord[0] > 0:
                                    diff = 180 - coord[0]
                                    out_coord[0] = -180 - diff
                            hole_coords.append(out_coord)
                        out_holes.append(LinearRing(hole_coords))
                    out_gdf.loc[i_geom, 'geometry'] = Polygon(out_coords, holes=out_holes)
                    i_geom += 1
                    #polys.append(Polygon(out_coords, holes=out_holes))
        else:
            #polys.append(row['geometry'])
            out_gdf.loc[i_geom, 'geometry'] = row['geometry']
            i_geom += 1
    #data_gdf['geometry'] = polys
    return out_gdf


def merge_utm_vecs_wgs84(input_files, output_file, output_lyr=None, out_format='GPKG',
                         n_hemi_utm_file=None, s_hemi_utm_file=None, width_thres=350):
    """
    A function which merges input files in UTM projections to the WGS84 projection cutting
    polygons which wrap from one side of the world to other (i.e., 180/-180 boundary).

    :param input_files: list of input files
    :param output_file: output vector file.
    :param output_lyr: output vector layer - only used if output format is GPKG
    :param out_format: output file format.
    :param n_utm_zones_vec: GPKG file with layer per zone (layer names: 01, 02, ... 59, 60) each projected in
                            the northern hemisphere UTM projections.
    :param s_utm_zone_vec: GPKG file with layer per zone (layer names: 01, 02, ... 59, 60) each projected in
                            the southern hemisphere UTM projections.
    :param width_thres: The threshold (default 350 degrees) for the width of a polygon for which
                        the polygons will be checked, looping through all the coordinates

    """
    import geopandas
    import pandas
    import rsgislib.tools.utm
    import tqdm
    import os

    if n_hemi_utm_file is None:
        install_prefix = __file__[:__file__.find('lib')]
        n_hemi_utm_file = os.path.join(install_prefix, "share", "rsgislib", "utm_zone_boundaries_lyrs_north.gpkg")
        if n_hemi_utm_file is None:
            raise Exception("An input is needed for n_hemi_utm_file. The RSGISLib installed version was not be found.")
    if s_hemi_utm_file is None:
        install_prefix = __file__[:__file__.find('lib')]
        s_hemi_utm_file = os.path.join(install_prefix, "share", "rsgislib", "utm_zone_boundaries_lyrs_south.gpkg")
        if s_hemi_utm_file is None:
            raise Exception("An input is needed for s_hemi_utm_file. The RSGISLib installed version was not be found.")

    rsgis_utils = rsgislib.RSGISPyUtils()
    first = True
    for file in tqdm.tqdm(input_files):
        lyrs = rsgislib.vectorutils.getVecLyrsLst(file)
        for lyr in lyrs:
            bbox = rsgis_utils.getVecLayerExtent(file, layerName=lyr)
            bbox_area = rsgis_utils.calc_bbox_area(bbox)
            if bbox_area > 0:
                vec_epsg = rsgis_utils.getProjEPSGFromVec(file, vecLyr=lyr)
                zone, hemi = rsgislib.tools.utm.utm_from_epsg(int(vec_epsg))
                zone_str = rsgis_utils.zero_pad_num_str(zone, str_len=2, round_num=False, round_n_digts=0,
                                                        integerise=True)

                if hemi.upper() == 'S':
                    utm_zones_file = s_hemi_utm_file
                else:
                    utm_zones_file = n_hemi_utm_file

                contained = vec_within_vec(utm_zones_file, zone_str, file, lyr)
                if not contained:
                    data_gdf = geopandas.read_file(file, layer=lyr)
                    utm_gdf = geopandas.read_file(utm_zones_file, layer=zone_str)

                    data_inter_gdf = geopandas.overlay(data_gdf, utm_gdf, how='intersection')
                    data_diff_gdf = geopandas.overlay(data_gdf, utm_gdf, how='difference')
                    if (len(data_inter_gdf) > 0) and (len(data_diff_gdf) > 0):
                        data_split_gdf = pandas.concat([data_inter_gdf, data_diff_gdf])
                    elif len(data_diff_gdf) > 0:
                        data_split_gdf = data_diff_gdf
                    else:
                        data_split_gdf = data_inter_gdf

                    if len(data_split_gdf) > 0:
                        data_gdf = data_split_gdf.to_crs("EPSG:4326")
                else:
                    data_gdf = geopandas.read_file(file, layer=lyr)
                    if len(data_gdf) > 0:
                        data_gdf = data_gdf.to_crs("EPSG:4326")

                if len(data_gdf) > 0:
                    data_gdf_bounds = data_gdf.bounds
                    widths = data_gdf_bounds['maxx'] - data_gdf_bounds['minx']
                    if widths.max() > 350:
                        data_gdf = geopd_check_polys_wgs84bounds_geometry(data_gdf, width_thres)
                    if first:
                        out_gdf = data_gdf
                        first = False
                    else:
                        out_gdf = out_gdf.append(data_gdf)

    if not first:
        if out_format == "GPKG":
            if output_lyr is None:
                raise Exception("If output format is GPKG then an output layer is required.")
            out_gdf.to_file(output_file, layer=output_lyr, driver=out_format)
        else:
            out_gdf.to_file(output_file, driver=out_format)


def get_files_mtime(file_lst, dt_before=None, dt_after=None):
    """
    A function which subsets a list of files based on datetime of
    last modification. The function also does a check as to whether
    a file exists, files which don't exist will be ignored.

    :param file_lst: The list of file path - represented as strings.
    :param dt_before: a datetime object with a date/time where files before this will be returned
    :param dt_after: a datetime object with a date/time where files after this will be returned

    """
    import os
    import datetime
    if (dt_before is None) and (dt_after is None):
        raise Exception("You must define at least one of dt_before or dt_after")
    out_file_lst = list()
    for cfile in file_lst:
        if os.path.exists(cfile):
            mod_time_stamp = os.path.getmtime(cfile)
            mod_time = datetime.datetime.fromtimestamp(mod_time_stamp)
            if (dt_before is not None) and (mod_time < dt_before):
                out_file_lst.append(cfile)
            if (dt_after is not None) and (mod_time > dt_after):
                out_file_lst.append(cfile)
    return out_file_lst


import datetime
input_vecs = glob.glob("/scratch/a.pfb/gmw_v2_gapfill/data/granule_vegmsks_vecs/*.gpkg")
#merge_vector_files(input_vecs, '/scratch/a.pfb/gmw_v2_gapfill/data/granule_veg_msks.gpkg', 'granule_veg_msks', 'GPKG', out_epsg=4326)

input_vecs_adds = get_files_mtime(input_vecs, dt_before=None, dt_after=datetime.datetime(year=2021, month=3, day=10))

print(input_vecs_adds)

merge_utm_vecs_wgs84(input_vecs_adds, '/scratch/a.pfb/gmw_v2_gapfill/data/granule_veg_msks_additions.gpkg', 'granule_veg_msks', out_format='GPKG',
                     n_hemi_utm_file='/scratch/a.pfb/gmw_v2_gapfill/data/meta_data/UTM_Zone_Boundaries_lyrs_north.gpkg',
                     s_hemi_utm_file='/scratch/a.pfb/gmw_v2_gapfill/data/meta_data/UTM_Zone_Boundaries_lyrs_south.gpkg')


rsgislib.vectorutils.mergeVectors2GPKG(['/scratch/a.pfb/gmw_v2_gapfill/data/granule_veg_msks_additions.gpkg',
                                        '/scratch/a.pfb/gmw_v2_gapfill/data/granule_veg_msks_bkup_20210302.gpkg'],
                                        '/scratch/a.pfb/gmw_v2_gapfill/data/granule_veg_msks.gpkg',
                                        'granule_veg_msks', False)


