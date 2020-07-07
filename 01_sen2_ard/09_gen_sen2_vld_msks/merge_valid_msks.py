import glob
import rsgislib.vectorutils


def merge_vector_files(input_files, output_file, output_lyr=None, out_format='GPKG', out_epsg=None):
    """
    A function which merges the input files into a single output file. If the input files have multiple
    layers they are all merged into the output file.

    :param input_files: list of input files
    :param output_file: output vector file.
    :param output_lyr: output vector layer.
    :param out_format: output file format.
    :param out_epsg: if input layers are different projections then option can be used to define the output
                     projection.

    """
    import tqdm
    import geopandas
    first = True
    for vec_file in tqdm.tqdm(input_files):
        lyrs = rsgislib.vectorutils.getVecLyrsLst(vec_file)
        for lyr in lyrs:
            if first:
                data_gdf = geopandas.read_file(vec_file, layer=lyr)
                if out_epsg is not None:
                    data_gdf = data_gdf.to_crs(epsg=out_epsg)
                first = False
            else:
                tmp_data_gdf = geopandas.read_file(vec_file, layer=lyr)
                if out_epsg is not None:
                    tmp_data_gdf = tmp_data_gdf.to_crs(epsg=out_epsg)

                data_gdf = data_gdf.append(tmp_data_gdf)

    if not first:
        if out_format == "GPKG":
            if output_lyr is None:
                raise Exception("If output format is GPKG then an output layer is required.")
            data_gdf.to_file(output_file, layer=output_lyr, driver=out_format)
        else:
            data_gdf.to_file(output_file, driver=out_format)


input_vecs = glob.glob("/scratch/a.pfb/gmw_v2_gapfill/data/granule_vld_msks/*.gpkg")
merge_vector_files(input_vecs, '/scratch/a.pfb/gmw_v2_gapfill/data/granule_vld_msks.gpkg', 'granule_vld_msks', 'GPKG', out_epsg=4326)


