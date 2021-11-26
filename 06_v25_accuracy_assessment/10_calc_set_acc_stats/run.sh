export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/Users/pete/Temp/rsgislib_lcl/lib
export PYTHONPATH=$PYTHONPATH:/Users/pete/Temp/rsgislib_lcl/lib/python3.9/site-packages

python calc_set_acc_stats.py
python summarise_acc_stats.py
