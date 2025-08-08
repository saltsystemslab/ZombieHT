#!/bin/bash

VARIANTS=(GZHM_ADAPTIVE)
#VARIANTS=(ICEBERG_SINGLE_THREAD CLHT CUCKOO)
UPDATE_PCTS=(50)
LOAD_FACTORS=(95)

for UPDATE_PCT in ${UPDATE_PCTS[@]}; do
for LOAD_FACTOR in ${LOAD_FACTORS[@]}; do
for VARIANT in ${VARIANTS[@]}; do

time ./bench/paper_final/churn_switch.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT}

done
done
done

