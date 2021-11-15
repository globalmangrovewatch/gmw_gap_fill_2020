singularity exec --bind /mangroves_server:/mangroves_server --bind /bigdata:/bigdata /bigdata/gmw_v25_acc_ass/sw_imgs/au-eoed-beta-dev.sif python /bigdata/gmw_v25_acc_ass/scripts/06_v25_accuracy_assessment/07_extract_ref_imgs/create_ls2010_imgs.py

singularity exec --bind /mangroves_server:/mangroves_server --bind /bigdata:/bigdata /bigdata/gmw_v25_acc_ass/sw_imgs/au-eoed-beta-dev.sif python /bigdata/gmw_v25_acc_ass/scripts/06_v25_accuracy_assessment/07_extract_ref_imgs/create_palsar2010_imgs.py


singularity exec --bind /mangroves_server:/mangroves_server --bind /bigdata:/bigdata /bigdata/gmw_v25_acc_ass/sw_imgs/au-eoed-beta-dev.sif python /bigdata/gmw_v25_acc_ass/scripts/06_v25_accuracy_assessment/07_extract_ref_imgs/calc_img_stats.py

