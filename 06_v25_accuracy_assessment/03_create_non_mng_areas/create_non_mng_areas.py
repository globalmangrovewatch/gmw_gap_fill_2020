import glob
import os
import numpy

import rsgislib.tools.filetools
import rsgislib.imagecalc
import rsgislib.rastergis
import rsgislib.rastergis.ratutils


def add_non_mng_cls(input_img, output_img, tmp_dir):
    basename = rsgislib.tools.filetools.get_file_basename(img)
    
    dist_to_mng_img = os.path.join(tmp_dir, "{}_dist.kea".format(basename))
    rsgislib.imagecalc.calc_dist_to_img_vals(input_img, dist_to_mng_img, 1, img_band=1, gdalformat='KEA', max_dist=200, no_data_val=201, unit_geo=False)
    
    band_defns = list()
    band_defns.append(rsgislib.imagecalc.BandDefn('mng', input_img, 1))
    band_defns.append(rsgislib.imagecalc.BandDefn('dist', dist_to_mng_img, 1))
    rsgislib.imagecalc.band_math(output_img, '(mng==1)?1:(dist<200)?2:0', 'KEA', rsgislib.TYPE_8UINT, band_defns)
    
    rsgislib.rastergis.pop_rat_img_stats(output_img, add_clr_tab=True, calc_pyramids=True, ignore_zero=True)
    rsgislib.rastergis.ratutils.set_column_data(output_img, "cls_name", numpy.array(["", "Mangrove", "Other"]))


out_dir = "/Users/pete/Temp/gmw_v25_extent/roi_cls_imgs"
tmp_dir = "/Users/pete/Temp/gmw_v25_extent/tmp"
imgs = glob.glob("/Users/pete/Temp/gmw_v25_extent/roi_imgs/*.kea")
for img in imgs:
    basename = rsgislib.tools.filetools.get_file_basename(img)
    out_img = os.path.join(out_dir, "{}_cls.kea".format(basename))
    add_non_mng_cls(img, out_img, tmp_dir)



