import pprint
import os
import shutil
import glob
import rsgislib.tools.filetools
import rsgislib.tools.utils
import rsgislib.vectorutils


sets_4_who = rsgislib.tools.utils.read_json_to_dict("../01_define_rois/whos_doing_what.json")

# Check that all the sets have someone allocated.
rois = dict()
for i in range(60):
    rois[i+1] = ""

for person in sets_4_who:
    for roi in sets_4_who[person]:
        rois[roi] = person

for i in range(60):
    if rois[i+1] == "":
        print("ROI {} does not have a person allocated to it".format(i+1))


roi_sets_base_dir = "/Users/pete/Temp/gmw_v25_extent/roi_cls_acc_pts_sets"
out_set_archs_dir = "./roi_sets_archs/"
tmp_dir = "tmp"
for person in sets_4_who:
    print(person)
    out_arch_file = os.path.join(out_set_archs_dir, "{}_roi_sets_1to5.tar.gz".format(person))
    out_file_lst = list()
    for roi in sets_4_who[person]:
        roi_sets_dir = os.path.join(roi_sets_base_dir, "gmw_acc_roi_{}_cls_acc_pts".format(roi))
        print("\t{}".format(roi_sets_dir))
        for set in [1,2,3,4,5]:
            set_file  = os.path.join(roi_sets_dir, "gmw_acc_roi_{}_cls_acc_pts_{}.geojson".format(roi, set))
            print("\t\t{}".format(set_file))
            out_file_lst.append(set_file)
            
    rsgislib.tools.filetools.create_targz_arch(out_arch_file, out_file_lst, base_path=roi_sets_base_dir)

