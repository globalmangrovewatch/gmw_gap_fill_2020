singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind \
/home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python chk_imgs.py \
--nbands 1 --rmerr --chkproj --epsg 4326 --readimg --npxls 1  \
-i "/scratch/a.pfb/gmw_v2_gapfill/data/gmw_tiles/gmw_ref_tiles/*.kea"





