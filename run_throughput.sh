#!/bin/bash

VARIANTS=(GRHM)
UPDATE_PCTS=(50 5)
LOAD_FACTORS=(95)
THROUGHPUT_FREQ=1

for UPDATE_PCT in ${UPDATE_PCTS[@]}; do
for LOAD_FACTOR in ${LOAD_FACTORS[@]}; do
for VARIANT in ${VARIANTS[@]}; do

echo "./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}"
time ./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}

done
done
done

VARIANTS=(GRHM GZHM_ADAPTIVE ABSL ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED TRHM RHM)
#VARIANTS=(ICEBERG_SINGLE_THREAD CLHT CUCKOO)
UPDATE_PCTS=(50)
LOAD_FACTORS=(75 85)
THROUGHPUT_FREQ=1

for UPDATE_PCT in ${UPDATE_PCTS[@]}; do
for LOAD_FACTOR in ${LOAD_FACTORS[@]}; do
for VARIANT in ${VARIANTS[@]}; do

echo "./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}"
time ./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}

done
done
done

VARIANTS=(GZHM_ADAPTIVE TRHM RHM)
UPDATE_PCTS=(50)
LOAD_FACTORS=(95)
THROUGHPUT_FREQ=1

for UPDATE_PCT in ${UPDATE_PCTS[@]}; do
for LOAD_FACTOR in ${LOAD_FACTORS[@]}; do
for VARIANT in ${VARIANTS[@]}; do

echo "./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}"
time ./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}

done
done
done

# Add 5\% update with number of churn cycles to be 1500 
