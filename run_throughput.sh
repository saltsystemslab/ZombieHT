#!/bin/bash
VARIANTS=(CLHT ABSL ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED ICEBERG_SINGLE_THREAD CLHT CUCKOO)
UPDATE_PCTS=(5 50)
LOAD_FACTORS=(75 85 95)
THROUGHPUT_FREQ=1
CHURN_CYCLES=320

for UPDATE_PCT in ${UPDATE_PCTS[@]}; do
for LOAD_FACTOR in ${LOAD_FACTORS[@]}; do
for VARIANT in ${VARIANTS[@]}; do

echo "./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}"
time ./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ} ${CHURN_CYCLES}
done
done
done

# Ordered Variants, 5% Update @ 95% Load factor (2000 cycles)
VARIANTS=(GRHM TRHM RHM GZHM_ADAPTIVE) 
UPDATE_PCTS=(5)
LOAD_FACTORS=(95)
THROUGHPUT_FREQ=1
CHURN_CYCLES=2000

for UPDATE_PCT in ${UPDATE_PCTS[@]}; do
for LOAD_FACTOR in ${LOAD_FACTORS[@]}; do
for VARIANT in ${VARIANTS[@]}; do

echo "./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}"
time ./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ} ${CHURN_CYCLES}

done
done
done

# Ordered Variants, 5% Update @ (75,85)% Load factor (320 cycles)
VARIANTS=(GRHM TRHM RHM GZHM_ADAPTIVE) 
UPDATE_PCTS=(5)
LOAD_FACTORS=(75 85)
THROUGHPUT_FREQ=1
CHURN_CYCLES=320

for UPDATE_PCT in ${UPDATE_PCTS[@]}; do
for LOAD_FACTOR in ${LOAD_FACTORS[@]}; do
for VARIANT in ${VARIANTS[@]}; do

echo "./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}"
time ./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ} ${CHURN_CYCLES}

done
done
done

# Ordered Variants, 50% Update @ (75, 85, 95)% Load factor (320 cycles)
VARIANTS=(GRHM TRHM RHM GZHM_ADAPTIVE)
UPDATE_PCTS=(50)
LOAD_FACTORS=(75 85 95)
THROUGHPUT_FREQ=1
CHURN_CYCLES=320

for UPDATE_PCT in ${UPDATE_PCTS[@]}; do
for LOAD_FACTOR in ${LOAD_FACTORS[@]}; do
for VARIANT in ${VARIANTS[@]}; do

echo "./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}"
time ./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ} ${CHURN_CYCLES}
done
done
done
