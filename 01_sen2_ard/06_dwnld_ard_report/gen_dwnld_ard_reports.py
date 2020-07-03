import sys
sys.path.insert(0, "../03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process


sen2_rcd_obj = RecordSen2Process("/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db")

print("Scenes to download:")
scns = sen2_rcd_obj.get_scns_download()
for scn in scns:
    print("\t{}".format(scn.product_id))

print("Scenes to ARD:")
scns = sen2_rcd_obj.get_scns_ard()
for scn in scns:
    print("\t{}".format(scn.product_id))


