#!/bin/bash

# Ordered hash tables.

## GZHM(C)
./bench/paper_final/churn_50_50.sh 3 0 GZHM_ADAPTIVE
./bench/paper_final/churn_25_75.sh 3 0 GZHM_ADAPTIVE
./bench/paper_final/churn_5_95.sh 3 0 GZHM_ADAPTIVE

## RobinHood HashMap (RHM)
./bench/paper_final/churn_50_50.sh 3 0 RHM
./bench/paper_final/churn_25_75.sh 3 0 RHM
./bench/paper_final/churn_5_95.sh 3 0 RHM

## RobinHood + Tombstone HashMap (TRHM)
./bench/paper_final/churn_50_50.sh 3 0 TRHM
./bench/paper_final/churn_25_75.sh 3 0 TRHM
./bench/paper_final/churn_5_95.sh 3 0 TRHM

## Graveyard Hashmap (GRHM)
./bench/paper_final/churn_50_50.sh 3 0 GRHM
./bench/paper_final/churn_25_75.sh 3 0 GRHM
./bench/paper_final/churn_5_95.sh 3 0 GRHM

# Unordered hash tables.

# GZHM(V)
./bench/paper_final/churn_50_50.sh 3 0 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED
./bench/paper_final/churn_25_75.sh 3 0 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED
./bench/paper_final/churn_5_95.sh 3 0 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED


# ICEBERG
./bench/paper_final/churn_50_50.sh 3 0 ICEBERG_SINGLE_THREAD
./bench/paper_final/churn_25_75.sh 3 0 ICEBERG_SINGLE_THREAD
./bench/paper_final/churn_5_95.sh 3 0 ICEBERG_SINGLE_THREAD

# CLHT
./bench/paper_final/churn_50_50.sh 3 0 CLHT
./bench/paper_final/churn_25_75.sh 3 0 CLHT
./bench/paper_final/churn_5_95.sh 3 0 CLHT

# LIBCUCKOO
./bench/paper_final/churn_50_50.sh 3 0 CUCKOO
./bench/paper_final/churn_25_75.sh 3 0 CUCKOO
./bench/paper_final/churn_5_95.sh 3 0 CUCKOO


