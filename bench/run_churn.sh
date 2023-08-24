#!/bin/bash

make clean hm_churn
./hm_churn -k 30 -q 24 -c 20 -l 1000 -d ./bench_run/rhm/

make clean hm_churn T=1 REBUILD_BY_CLEAR=1
./hm_churn -k 30 -q 24 -c 20 -l 1000 -d ./bench_run/trhm/

make clean hm_churn T=1 REBUILD_AMORTIZED_GRAVEYARD=1
./hm_churn -k 30 -q 24 -c 20 -l 1000 -d ./bench_run/grhm/

make clean hm_churn T=1 REBUILD_DEAMORTIZED_GRAVEYARD=1
./hm_churn -k 30 -q 24 -c 20 -l 1000 -d ./bench_run/gzhm/

./bench/plot_graph.py
