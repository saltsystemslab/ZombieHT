#!/bin/bash

# run_args="-k 36 -q 20 -v 0 -c 100 -l 3000 -i 95 -s 1"
run_args="-k 38 -q 22 -v 0 -c 100 -l 10000 -i 95 -s 1"
# run_args="-k 43 -q 27 -v 0 -c 100 -l 400000 -i 95 -s 1"
# run_args="-k 38 -q 30 -v 0 -c 100 -l 40000 -i 95 -s 1"

QF_BITS_PER_SLOT=16

if [ -z "$1" ]; then
    out_dir="bench_run"
else
    out_dir="$1"
fi

rm -rf $out_dir/*

make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=GZHM
./hm_churn $run_args -d $out_dir/gzhm/

# make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=GZHM_NO_INSERT
# ./hm_churn $run_args -d $out_dir/gzhm-no-ins/

# make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=GZHM_DELETE PTS=2
# ./hm_churn $run_args -d $out_dir/gzhm-delete_2/

# make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=GZHM_DELETE PTS=1.5
# ./hm_churn $run_args -d $out_dir/gzhm-delete_1.5/

make clean hm_churn VAR=GZHM_DELETE PTS=1
./hm_churn $run_args -d $out_dir/gzhm-delete_slot0/

make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=GZHM_DELETE PTS=1
./hm_churn $run_args -d $out_dir/gzhm-delete_1/

make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=GZHM_DELETE PTS=0.5
./hm_churn $run_args -d $out_dir/gzhm-delete_0.5/

# make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=GZHM_INSERT
# ./hm_churn $run_args -d $out_dir/gzhm_insert/

make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=RHM
./hm_churn $run_args -d $out_dir/rhm/

make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=TRHM
./hm_churn $run_args -d $out_dir/trhm/

make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=GRHM
./hm_churn $run_args -d $out_dir/grhm/

# make clean hm_churn QF_BITS_PER_SLOT=$QF_BITS_PER_SLOT VAR=GRHM_NO_INSERT
# ./hm_churn $run_args -d $out_dir/grhm-no-ins/

python3 ./bench/plot_graph.py $out_dir
