#!/bin/bash

# run_args="-k 36 -q 20 -v 0 -c 100 -l 3000 -i 95 -s 1"
run_args="-k 38 -q 22 -v 0 -c 1000 -l 10000 -i 95 -s 1"
# run_args="-k 43 -q 27 -v 0 -c 100 -l 400000 -i 95 -s 1"
# run_args="-k 38 -q 30 -v 0 -c 100 -l 40000 -i 95 -s 1"

if [ -z "$1" ]; then
    out_dir="bench_run"
else
    out_dir="$1"
fi

rm -rf $out_dir/*

make clean hm_churn VAR=GZHM
./hm_churn $run_args -d $out_dir/gzhm/

# make clean hm_churn VAR=GZHM_NO_INSERT
# ./hm_churn $run_args -d $out_dir/gzhm-no-ins/

make clean hm_churn VAR=GZHM_DELETE
./hm_churn $run_args -d $out_dir/gzhm-delete/

# make clean hm_churn VAR=GZHM_INSERT
# ./hm_churn $run_args -d $out_dir/gzhm_insert/

make clean hm_churn VAR=RHM
./hm_churn $run_args -d $out_dir/rhm/

# make clean hm_churn VAR=TRHM
# ./hm_churn $run_args -d $out_dir/trhm/

make clean hm_churn VAR=GRHM
./hm_churn $run_args -d $out_dir/grhm/

# make clean hm_churn VAR=GRHM_NO_INSERT
# ./hm_churn $run_args -d $out_dir/grhm-no-ins/

python3 ./bench/plot_graph.py $out_dir
