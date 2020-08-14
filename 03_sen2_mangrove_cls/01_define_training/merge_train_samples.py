import rsgislib.vectorutils
import glob

input_files = glob.glob("/scratch/a.pfb/gmw_v2_gapfill/data/granule_mang_train_vecs/*.gpkg")
output_file = "/scratch/a.pfb/gmw_v2_gapfill/data/granule_mang_train_smpls.gpkg"
#rsgislib.vectorutils.merge_vector_files(input_files, output_file, output_lyr="samples", out_format='GPKG', out_epsg=4326)

def add_numeric_col_lut(vec_file, vec_lyr, ref_col, val_lut, out_col, vec_out_file, vec_out_lyr, out_format='GPKG'):
    """
        A function which adds a numeric column based off an existing column in the vector file,
        using an dict LUT to define the values.

        :param vec_file: Input vector file.
        :param vec_lyr: Input vector layer within the input file.
        :param ref_col: The column within which the unique values will be identified.
        :param val_lut: A dict LUT (key should be value in ref_col and value be the value outputted to out_col).
        :param out_col: The output numeric column
        :param vec_out_file: Output vector file
        :param vec_out_lyr: output vector layer name.
        :param out_format: output file format (default GPKG).

        """
    import geopandas
    import numpy
    # Open vector file
    base_gpdf = geopandas.read_file(vec_file, layer=vec_lyr)
    # Add output column
    base_gpdf[out_col] = numpy.zeros((base_gpdf.shape[0]), dtype=int)
    # Loop values in LUT
    for lut_key in val_lut:
        sel_rows = base_gpdf[ref_col] == lut_key
        base_gpdf.loc[sel_rows, out_col] = val_lut[lut_key]

    if out_format == 'GPKG':
        base_gpdf.to_file(vec_out_file, layer=vec_out_lyr, driver=out_format)
    else:
        base_gpdf.to_file(vec_out_file, driver=out_format)


vec_file = '/scratch/a.pfb/gmw_v2_gapfill/data/granule_mang_train_smpls.gpkg'
vec_lyr = 'samples'
ref_col = 'Class'
val_lut = dict()
val_lut['mangrove'] = 1
val_lut['other'] = 2
out_col = 'ClassID'
vec_out_file = '/scratch/a.pfb/gmw_v2_gapfill/data/granule_mang_train_smpls_uid.gpkg'
vec_out_lyr = 'samples'
add_numeric_col_lut(vec_file, vec_lyr, ref_col, val_lut, out_col, vec_out_file, vec_out_lyr, out_format='GPKG')
