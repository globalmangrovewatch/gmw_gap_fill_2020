import sys
sys.path.insert(0, "../03_find_dwnld_scns")
from sen2scnprocess import RecordSen2Process


sen2_rcd_obj = RecordSen2Process("/scratch/a.pfb/gmw_v2_gapfill/scripts/01_sen2_ard/03_find_dwnld_scns/sen2_scn.db")

#sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20200203T221929_N0208_R029_T01KAB_20200203T233138")

"""
sen2_rcd_obj.reset_ard_scn("S2A_MSIL1C_20200119T221931_N0208_R029_T01KAT_20200119T232142", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200503T221939_N0209_R029_T01KBB_20200503T233443", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20180217T215909_N0206_R086_T01KHV_20180217T230829", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200510T151339_N0209_R125_T20SKA_20200514T144523", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200321T151339_N0209_R125_T20SKA_20200321T200639", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200410T151339_N0209_R125_T20SKA_20200410T200753", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200430T151339_N0209_R125_T20SKA_20200430T183223", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200101T151339_N0208_R125_T20SKA_20200101T165111", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200619T151349_N0209_R125_T20SKA_20200619T183608", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200331T151339_N0209_R125_T20SKA_20200331T183134", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200121T151339_N0208_R125_T20SKA_20200121T183155", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200111T151339_N0208_R125_T20SKA_20200111T165115", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200609T151349_N0209_R125_T20SKA_20200609T183457", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2A_MSIL1C_20171214T014701_N0206_R074_T52NFF_20171214T064123", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200206T015839_N0209_R060_T52RFV_20200206T035042", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20181001T014649_N0206_R017_T52RGV_20181001T033136", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2A_MSIL1C_20200404T004701_N0209_R102_T54MZS_20200404T022353", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2A_MSIL1C_20190106T010301_N0207_R045_T54PZV_20190106T022736", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2B_MSIL1C_20200416T223009_N0209_R072_T60KXC_20200416T234045", delpath=True)
sen2_rcd_obj.reset_ard_scn("S2A_MSIL1C_20200411T223011_N0209_R072_T60KXC_20200411T235019", delpath=True)
"""

sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20190621T223019_N0207_R072_T60KXG_20190621T233904")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20191017T232209_N0208_R044_T58NHM_20191018T004105")
sen2_rcd_obj.reset_all_scn("S2A_MSIL1C_20180603T230911_N0206_R101_T58LGN_20180604T003215")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20200506T205039_N0209_R071_T04KDE_20200506T234006")
sen2_rcd_obj.reset_all_scn("S2A_MSIL1C_20200205T220911_N0209_R129_T01KAU_20200205T231255")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20180907T073219_N0206_R106_T37KFR_20180907T123710")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20190318T231849_N0207_R001_T58LFN_20190319T002947")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20191208T053709_N0208_R062_T43NBB_20191208T085320")
sen2_rcd_obj.reset_all_scn("S2A_MSIL1C_20200322T053711_N0209_R062_T43NCD_20200322T081459")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20190815T231859_N0208_R001_T58LFP_20190816T002847")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20171217T210909_N0206_R057_T04QFJ_20171218T000654")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20191114T205939_N0208_R014_T05QKB_20191114T221006")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20190726T163319_N0208_R140_T15MXV_20190726T192824")
sen2_rcd_obj.reset_all_scn("S2A_MSIL1C_20180308T163301_N0206_R140_T15NYA_20180308T211755")
sen2_rcd_obj.reset_all_scn("S2A_MSIL1C_20181109T065031_N0207_R077_T39MYQ_20181109T082915")
sen2_rcd_obj.reset_all_scn("S2A_MSIL1C_20200208T221931_N0209_R029_T60KXD_20200208T232441")
sen2_rcd_obj.reset_all_scn("S2A_MSIL1C_20180326T155221_N0206_R111_T17MPS_20180326T220537")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20180321T155219_N0206_R111_T17MPS_20180321T190526")
sen2_rcd_obj.reset_all_scn("S2A_MSIL1C_20180316T155221_N0206_R111_T17MPS_20180316T203233")
sen2_rcd_obj.reset_all_scn("S2B_MSIL1C_20200107T153619_N0208_R068_T17NRE_20200107T185347")
