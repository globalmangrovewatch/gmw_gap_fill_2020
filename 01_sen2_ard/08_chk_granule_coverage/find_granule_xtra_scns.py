import glob
import os
import rsgislib

rsgis_utils = rsgislib.RSGISPyUtils()

out_g90_file = '../granule_xtra_scns_g90.txt'
out_l90_file = '../granule_xtra_scns_l90.txt'
granule_files = glob.glob('/scratch/a.pfb/gmw_v2_gapfill/data/granule_cover/*.json')
out_g90_granules = []
out_l90_granules = []
completed_granules = []

for granule_file in granule_files:
    granule = rsgis_utils.get_file_basename(granule_file)
    print(granule)
    print("\t{}".format(granule_file))

    granule_info = rsgis_utils.readJSON2Dict(granule_file)
    print(granule_info)
    if granule_info['n_vld_roi_pxls'] > 0:
        prop_roi_pxls = (granule_info['n_clrsky_roi_pxls'] / granule_info['n_vld_roi_pxls']) * 100
        print("\t{}".format(round(prop_roi_pxls, 4)))
        if prop_roi_pxls > 99.9:
            completed_granules.append(granule)
        elif prop_roi_pxls > 90:
            out_g90_granules.append(granule)
        else:
            out_l90_granules.append(granule)

rsgis_utils.writeList2File(out_g90_granules, out_g90_file)
rsgis_utils.writeList2File(out_l90_granules, out_l90_file)

print("Completed ({} granules):".format(len(completed_granules)))
for granule in completed_granules:
    print("\t{}".format(granule))
