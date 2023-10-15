#!/bin/bash

rm -rf ./bench_run/*

make clean hm_churn T=1 REBUILD_BY_CLEAR=1
./hm_churn -k 30 -q 24 -c 20 -w 1000 -e 1000 -d ./bench_run/old_offset/

make clean hm_churn T=1 REBUILD_BY_CLEAR=1 BLOCKOFFSET_4_NUM_RUNENDS=1
./hm_churn -k 30 -q 24 -c 20 -w 1000 -e 1000 -d ./bench_run/new_offset/

./bench/plot_graph.py
