import rsgislib.vectorutils

"""
train_edit_vecs = [rsgislib.vectorutils.VecLayersInfoObj(vecfile='gmw_gap_fill_train_edits_mangroves.gpkg', veclyr='mangrove_pts', outlyr='mangrove_pts'), 
                   rsgislib.vectorutils.VecLayersInfoObj(vecfile='gmw_gap_fill_train_edits_other.gpkg', veclyr='other_pts', outlyr='other_pts'),
                   rsgislib.vectorutils.VecLayersInfoObj(vecfile='gmw_gap_fill_train_edits_not_mangroves.gpkg', veclyr='not_mangrove_regions', outlyr='not_mangrove_regions'),
                   rsgislib.vectorutils.VecLayersInfoObj(vecfile='gmw_gap_fill_train_edits_not_other.gpkg', veclyr='not_other_regions', outlyr='not_other_regions')]

output_file = 'gmw_gap_fill_train_edits_dfds.gpkg'

rsgislib.vectorutils.merge_to_multi_layer_vec(train_edit_vecs, output_file, format='GPKG', overwrite=True)
"""


inFileList = ['gmw_gap_fill_train_edits_mangroves.gpkg', 'gmw_gap_fill_train_edits_other.gpkg', 'gmw_gap_fill_train_edits_not_mangroves.gpkg', 'gmw_gap_fill_train_edits_not_other.gpkg']
rsgislib.vectorutils.mergeVectors2GPKGIndLyrs(inFileList, 'gmw_gap_fill_train_edits.gpkg', rename_dup_lyrs=False, geom_type=None)
