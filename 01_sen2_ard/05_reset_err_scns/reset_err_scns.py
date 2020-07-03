sys.path.insert(0, "../03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process


sen2_rcd_obj = RecordSen2Process("/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db")

sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20200203T221929_N0208_R029_T01KAB_20200203T233138")