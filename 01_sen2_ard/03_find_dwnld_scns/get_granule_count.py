import rsgislib

from sen2scnprocess import RecordSen2Process

sen2_rcd_obj = RecordSen2Process('/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db')

rsgis_utils = rsgislib.RSGISPyUtils()
granule_lst = rsgis_utils.readTextFile2List('/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/sen2_roi_granule_lst.txt')

total_scns = 0
for granule in granule_lst:
    scns = sen2_rcd_obj.granule_scns(granule)

    print("{}: {}".format(granule, len(scns)))

    total_scns += len(scns)

print("Total Scenes: {}".format(total_scns))

