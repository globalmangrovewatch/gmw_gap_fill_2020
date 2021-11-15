import glob

import rsgislib
import rsgislib.imageutils




def subset_to_geoms_bbox(
    input_img: str,
    vec_file: str,
    vec_lyr: str,
    att_unq_val_col: str,
    out_img_base: str,
    gdalformat: str = "KEA",
    datatype: int = None,
    out_img_ext: str = "kea",
):
    """
    Subset an image to the bounding box of a each geometry in the input vector
    producing multiple output files. Useful for splitting an image into tiles
    of unequal sizes or extracting sampling plots from a larger image.

    :param input_img: The input image from which the subsets will be extracted.
    :param vec_file: input vector file/path
    :param vec_lyr: input vector layer name
    :param att_unq_val_col: column within the attribute table which has a value
                            to be included within the output file name so the
                            output files can be identified and have unique file
                            names.
    :param out_img_base: the output images base path and file name
    :param gdalformat: output image file format (default: KEA)
    :param datatype: output image data type. If None (default) then taken from
                     the input image.
    :param out_img_ext: output image file extension (e.g., kea)

    """
    import rsgislib.vectorgeoms
    import rsgislib.vectorattrs
    import rsgislib.tools.geometrytools

    if datatype is None:
        datatype = rsgislib.imageutils.get_rsgislib_datatype_from_img(input_img)

    bboxs = rsgislib.vectorgeoms.get_geoms_as_bboxs(vec_file, vec_lyr)
    print(bboxs)
    print(
        "There are {} geometries for "
        "which subsets will be created".format(len(bboxs))
    )

    unq_bbox_ids = rsgislib.vectorattrs.read_vec_column(
        vec_file, vec_lyr, att_unq_val_col
    )
    
    in_img_bbox = rsgislib.imageutils.get_img_bbox(input_img)

    for bbox_id, bbox in zip(unq_bbox_ids, bboxs):
        output_img = "{}{}.{}".format(out_img_base, bbox_id, out_img_ext)
        print(output_img)
        if rsgislib.tools.geometrytools.does_bbox_contain(in_img_bbox, bbox):
            rsgislib.imageutils.subset_bbox(
                input_img,
                output_img,
                gdalformat,
                datatype,
                bbox[0],
                bbox[1],
                bbox[2],
                bbox[3],
            )
        elif rsgislib.tools.geometrytools.do_bboxes_intersect(in_img_bbox, bbox):
            inter_bbox = rsgislib.tools.geometrytools.bbox_intersection(in_img_bbox, bbox)
            rsgislib.imageutils.subset_bbox(
                input_img,
                output_img,
                gdalformat,
                datatype,
                inter_bbox[0],
                inter_bbox[1],
                inter_bbox[2],
                inter_bbox[3],
            )
        


rsgislib.imageutils.set_env_vars_lzw_gtiff_outs()


input_imgs = glob.glob("/mangroves_server/mangroves/global_mangrove_watch_original/Projects/LandsatCompsMskedTIF/*.tif")
vrt_glb_img = "/bigdata/gmw_v25_acc_ass/gmw_landsat.vrt"

rsgislib.imageutils.create_mosaic_images_vrt(input_imgs, vrt_glb_img)


vec_file = "../01_define_rois/roi_centre_bboxs_roi_ids.geojson"
vec_lyr = "roi_centre_bboxs_roi_ids"
out_img_base = "/bigdata/gmw_v25_acc_ass/landsat_acc_rois/gmw_ls_acc_roi_"

subset_to_geoms_bbox(vrt_glb_img, vec_file, vec_lyr, "roi_id", out_img_base, "GTIFF", None, "tif")


    