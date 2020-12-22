import rsgislib
import sys
import os

sys.path.insert(0, "../03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process


def keep_file(input_file):
    known_file_end = False
    if 'clearsky.kea' in input_file:
        known_file_end = True
    elif 'clearsky_refine.kea' in input_file:
        known_file_end = True
    elif 'clouds.kea' in input_file:
        known_file_end = True
    elif 'meta.json' in input_file:
        known_file_end = True
    elif 'valid.kea' in input_file:
        known_file_end = True
    elif 'stdsref.kea' in input_file:
        known_file_end = True
    return known_file_end

def clean_files():
    rsgis_utils = rsgislib.RSGISPyUtils()
    granule_lst = rsgis_utils.readTextFile2List('/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/sen2_roi_granule_lst.txt')

    sen2_rcd_obj = RecordSen2Process('/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db')
    for granule in granule_lst:
        print(granule)
        scns = sen2_rcd_obj.granule_scns(granule)
        for scn in scns:
            print("\t{}".format(scn.product_id))
            if scn.ard:
                for f in os.listdir(scn.ard_path):
                    file_path = os.path.join(scn.ard_path, f)
                    if os.path.isfile(file_path):
                        print("Found: {}".format(file_path))
                        if not keep_file(f):
                            print("\tDelete {}".format(file_path))
                            os.remove(file_path)

clean_files()

