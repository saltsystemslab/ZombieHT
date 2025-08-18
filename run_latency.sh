#!/bin/bash

# Ordered hash tables.

## GZHM(C)
./bench/paper_final/churn_5_95.sh 3 1 GZHM_ADAPTIVE

## RobinHood HashMap (RHM)
./bench/paper_final/churn_5_95.sh 3 1 RHM

## RobinHood + Tombstone HashMap (TRHM)
./bench/paper_final/churn_5_95.sh 3 1 TRHM

## Graveyard Hashmap (GRHM)
./bench/paper_final/churn_5_95.sh 3 1 GRHM

# Unordered hash tables.

# ABSL
./bench/paper_final/churn_5_95.sh 3 1 ABSL

# GZHM(V)
./bench/paper_final/churn_5_95.sh 3 1 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED

# ICEBERG
./bench/paper_final/churn_5_95.sh 3 1 ICEBERG_SINGLE_THREAD

# CLHT
./bench/paper_final/churn_5_95.sh 3 1 CLHT

# LIBCUCKOO
./bench/paper_final/churn_5_95.sh 3 1 CUCKOO