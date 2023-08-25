#!/bin/bash

rm -rf ./bench_run/*

make clean hm_churn BLOCKOFFSET_4_NUM_RUNENDS=1 T=1 REBUILD_DEAMORTIZED_GRAVEYARD=1
./hm_churn -k 36 -q 20 -v 0 -c 100 -l 4000 -i 95 -s 1 -d ./bench_run/gzhm/

make clean hm_churn BLOCKOFFSET_4_NUM_RUNENDS=1 T=1 REBUILD_AT_INSERT=1
./hm_churn -k 36 -q 20 -v 0 -c 100 -l 4000 -i 95 -s 1 -d ./bench_run/gzhm_insert/

make clean hm_churn BLOCKOFFSET_4_NUM_RUNENDS=1
./hm_churn -k 36 -q 20 -v 0 -c 100 -l 4000 -i 95 -s 1 -d ./bench_run/rhm/

make clean hm_churn BLOCKOFFSET_4_NUM_RUNENDS=1 T=1
./hm_churn -k 36 -q 20 -v 0 -c 100 -l 4000 -i 95 -s 1 -d ./bench_run/trhm/

make clean hm_churn BLOCKOFFSET_4_NUM_RUNENDS=1 T=1 REBUILD_AMORTIZED_GRAVEYARD=1
./hm_churn -k 36 -q 20 -v 0 -c 100 -l 4000 -i 95 -s 1 -d ./bench_run/grhm/


./bench/plot_graph.py
