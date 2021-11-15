import rsgislib.imageutils
import glob


def calc_stats(in_img_path, no_data=0):
    imgs = glob.glob(in_img_path)
    for img in imgs:
        rsgislib.imageutils.pop_img_stats(img, use_no_data=True, no_data_val=no_data, calc_pyramids=True)



calc_stats("/bigdata/gmw_v25_acc_ass/landsat_acc_rois/gmw_ls_acc_roi_*.tif", no_data=0)
calc_stats("/bigdata/gmw_v25_acc_ass/palsar10_acc_rois/gmw_palsar10_acc_roi_*.tif", no_data=999)

