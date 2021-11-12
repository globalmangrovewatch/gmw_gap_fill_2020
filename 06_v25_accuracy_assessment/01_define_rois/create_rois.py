import rsgislib.vectorattrs
import rsgislib.vectorutils.createvectors


rsgislib.vectorutils.createvectors.create_bboxs_for_pts("roi_centre_pts.geojson", "roi_centre_pts", 0.2, 0.2, "roi_centre_bboxs.geojson", "roi_centre_bboxs",  out_format="GeoJSON", del_exist_vec=True, epsg_code=None)

rsgislib.vectorattrs.add_fid_col("roi_centre_bboxs.geojson", "roi_centre_bboxs", "roi_centre_bboxs_roi_ids.geojson", "roi_centre_bboxs_roi_ids", out_format="GeoJSON", out_col="roi_id")