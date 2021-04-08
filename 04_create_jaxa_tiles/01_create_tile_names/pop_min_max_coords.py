import geopandas


def addGeomBBOXCols(vec_file, vec_lyr, vec_out_file, vec_out_lyr, out_format='GPKG', min_x_col='MinX', max_x_col='MaxX', min_y_col='MinY', max_y_col='MaxY'):

    base_gpdf = geopandas.read_file(vec_file, layer=vec_lyr)
    
    geom_bounds = base_gpdf['geometry'].bounds
    
    base_gpdf[min_x_col] = geom_bounds['minx']
    base_gpdf[max_x_col] = geom_bounds['maxx']
    base_gpdf[min_y_col] = geom_bounds['miny']
    base_gpdf[max_y_col] = geom_bounds['maxy']
    
    if out_format == 'GPKG':
        base_gpdf.to_file(vec_out_file, layer=vec_out_lyr, driver=out_format)
    else:
        base_gpdf.to_file(vec_out_file, driver=out_format)
    

#addGeomBBOXCols('GMWDegreeTiles.gpkg', 'GMWDegreeTiles', 'GMWDegreeTilesBBOX.gpkg', 'GMWDegreeTiles', out_format='GPKG')

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


def createNameCol(vec_file, vec_lyr, vec_out_file, vec_out_lyr, out_format='GPKG', out_col='names', x_col='MinX', y_col='MaxY', prefix='', postfix='', latlong=True, int_coords=True, zero_x_pad=0, zero_y_pad=0, round_sig=0, non_neg=False):
    """
    A function which creates 
    """
    import geopandas
    import numpy
    import tqdm
    import rsgislib
    
    rsgis_utils = rsgislib.RSGISPyUtils()
    
    base_gpdf = geopandas.read_file(vec_file, layer=vec_lyr)
    
    names = list()
    for i in tqdm.tqdm(range(base_gpdf.shape[0])):
        x_col_val = base_gpdf.loc[i][x_col]
        y_col_val = base_gpdf.loc[i][y_col]
        
        if int_coords:
            x_col_val = int(x_col_val)
            y_col_val = int(y_col_val)
        
        x_col_val_neg = False
        y_col_val_neg = False
        if non_neg:
            if x_col_val < 0:
                x_col_val_neg = True
                x_col_val = x_col_val * (-1)
            if y_col_val < 0:
                y_col_val_neg = True
                y_col_val = y_col_val * (-1)
        
        if zero_x_pad > 0:
            x_col_val_str = zero_pad_num_str(x_col_val, str_len=zero_x_pad, round_num=False, round_n_digts=0, integerise=int_coords)
        else:
            x_col_val_str = '{}'.format(x_col_val)
            
        if zero_y_pad > 0:
            y_col_val_str = zero_pad_num_str(y_col_val, str_len=zero_y_pad, round_num=False, round_n_digts=0, integerise=int_coords)
        else:
            y_col_val_str = '{}'.format(y_col_val)
        
        if latlong:
            hemi = 'N'
            if y_col_val_neg:
                hemi = 'S'
            east_west = 'E'
            if x_col_val_neg:
                east_west = 'W'
            
            name = '{}{}{}{}{}{}'.format(prefix, hemi, y_col_val_str, east_west, x_col_val_str, postfix)
        else:
            name = '{}E{}N{}{}'.format(prefix, x_col_val_str, y_col_val_str, postfix)
        
        names.append(name)
    
    base_gpdf[out_col] = numpy.array(names)
        
    if out_format == 'GPKG':
        base_gpdf.to_file(vec_out_file, layer=vec_out_lyr, driver=out_format)
    else:
        base_gpdf.to_file(vec_out_file, driver=out_format)
    

#createNameCol('GMWDegreeTilesBBOX.gpkg', 'GMWDegreeTiles', 'GMWDegreeTilesBBOXName.gpkg', 'GMWDegreeTiles', out_format='GPKG', out_col='gmw_name', x_col='MinX', y_col='MaxY', prefix='GMW_', postfix='', latlong=True, int_coords=True, zero_x_pad=3, zero_y_pad=2, round_sig=0, non_neg=True)

createNameCol('GMWDegreeTilesBBOXName.gpkg', 'GMWDegreeTiles', 'GMWDegreeTilesBBOXName.gpkg', 'GMWDegreeTiles', out_format='GPKG', out_col='tile_name', x_col='MinX', y_col='MaxY', prefix='', postfix='', latlong=True, int_coords=True, zero_x_pad=3, zero_y_pad=2, round_sig=0, non_neg=True)

