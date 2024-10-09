#!/bin/bash

# Ordered hash tables.

## GZHM(C)
./bench/paper_final/churn_50_50.sh 3 1 GZHM_ADAPTIVE
./bench/paper_final/churn_25_75.sh 3 1 GZHM_ADAPTIVE
./bench/paper_final/churn_5_95.sh 3 1 GZHM_ADAPTIVE

## RobinHood HashMap (RHM)
./bench/paper_final/churn_50_50.sh 3 1 RHM
./bench/paper_final/churn_25_75.sh 3 1 RHM
./bench/paper_final/churn_5_95.sh 3 1 RHM

## RobinHood + Tombstone HashMap (TRHM)
./bench/paper_final/churn_50_50.sh 3 1 TRHM
./bench/paper_final/churn_25_75.sh 3 1 TRHM
./bench/paper_final/churn_5_95.sh 3 1 TRHM

## Graveyard Hashmap (GRHM)
./bench/paper_final/churn_50_50.sh 3 1 GRHM
./bench/paper_final/churn_25_75.sh 3 1 GRHM
./bench/paper_final/churn_5_95.sh 3 1 GRHM

# Unordered hash tables.

# GZHM(V)
./bench/paper_final/churn_50_50.sh 3 1 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED
./bench/paper_final/churn_25_75.sh 3 1 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED
./bench/paper_final/churn_5_95.sh 3 1 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED


# ICEBERG
./bench/paper_final/churn_50_50.sh 3 1 ICEBERG_SINGLE_THREAD
./bench/paper_final/churn_25_75.sh 3 1 ICEBERG_SINGLE_THREAD
./bench/paper_final/churn_5_95.sh 3 1 ICEBERG_SINGLE_THREAD

# CLHT
./bench/paper_final/churn_50_50.sh 3 1 CLHT
./bench/paper_final/churn_25_75.sh 3 1 CLHT
./bench/paper_final/churn_5_95.sh 3 1 CLHT

# LIBCUCKOO
./bench/paper_final/churn_50_50.sh 3 1 CUCKOO
./bench/paper_final/churn_25_75.sh 3 1 CUCKOO
./bench/paper_final/churn_5_95.sh 3 1 CUCKOO