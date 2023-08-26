#!/bin/bash

run_args="-k 36 -q 20 -v 0 -c 100 -l 4000 -i 95 -s 1"
# run_args="-k 43 -q 27 -v 0 -c 100 -l 400000 -i 95 -s 1"
# run_args="-k 38 -q 30 -v 0 -c 100 -l 40000 -i 95 -s 1"

if [ -z "$1" ]; then
    out_dir="bench_run"
else
    out_dir="$1"
fi

set -x

rm -rf $out_dir/*

make clean hm_churn BLOCKOFFSET_4_NUM_RUNENDS=1 T=1 REBUILD_DEAMORTIZED_GRAVEYARD=1
./hm_churn $run_args -d $out_dir/gzhm/

make clean hm_churn BLOCKOFFSET_4_NUM_RUNENDS=1 T=1 REBUILD_AT_INSERT=1
./hm_churn $run_args -d $out_dir/gzhm_insert/

make clean hm_churn BLOCKOFFSET_4_NUM_RUNENDS=1
./hm_churn $run_args -d $out_dir/rhm/

make clean hm_churn BLOCKOFFSET_4_NUM_RUNENDS=1 T=1
./hm_churn $run_args -d $out_dir/trhm/

make clean hm_churn BLOCKOFFSET_4_NUM_RUNENDS=1 T=1 REBUILD_AMORTIZED_GRAVEYARD=1
./hm_churn $run_args -d $out_dir/grhm/

./bench/plot_graph.py $out_dir
