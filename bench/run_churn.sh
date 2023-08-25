#!/bin/bash

rm -rf ./bench_run/*

make clean hm_churn T=1 REBUILD_DEAMORTIZED_GRAVEYARD=1
./hm_churn -k 43 -q 27 -v 0 -c 100 -l 400000 -i 95 -s 1 -d ./bench_run/gzhm/

make clean hm_churn
./hm_churn -k 43 -q 27 -v 0 -c 100 -l 400000 -i 95 -s 1 -d ./bench_run/rhm/

make clean hm_churn T=1 REBUILD_BY_CLEAR=1
./hm_churn -k 43 -q 27 -v 0 -c 100 -l 400000 -i 95 -s 1 -d ./bench_run/trhm/

make clean hm_churn T=1 REBUILD_AMORTIZED_GRAVEYARD=1
./hm_churn -k 43 -q 27 -v 0 -c 100 -l 400000 -i 95 -s 1 -d ./bench_run/grhm/


./bench/plot_graph.py
