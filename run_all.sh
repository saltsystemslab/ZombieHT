#!/bin/bash

#./bench/paper/gzhm_external.sh 2 0
#./bench/paper/gzhm_external.sh 2 1
# ./bench/paper/gzhm_variants.sh 2 0
#./bench/paper/gzhm_variants.sh 2 1
#./bench/paper/gzhm_pts_study.sh 2 0
#./bench/paper/gzhm_delete_study.sh 2 0

#./bench/paper_final/churn.sh 3 0 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED
#./bench/paper_final/churn.sh 3 0 ICEBERG_SINGLE_THREAD
#./bench/paper_final/churn.sh 3 0 ICEBERG
#./bench/paper_final/churn.sh 3 0 CUCKOO
#./bench/paper_final/churn.sh 3 0 CLHT


#./bench/paper_final/churn_50_50.sh 3 0 ABSL
#./bench/paper_final/churn_25_75.sh 3 0 ABSL
#./bench/paper_final/churn_5_95.sh 3 0 ABSL


#./bench/paper_final/churn_50_50.sh 3 0 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED
#./bench/paper_final/churn_25_75.sh 3 0 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED
#./bench/paper_final/churn_5_95.sh 3 0 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED

#./bench/paper_final/churn_50_50.sh 3 0 ICEBERG_SINGLE_THREAD
#./bench/paper_final/churn_25_75.sh 3 0 ICEBERG_SINGLE_THREAD
#./bench/paper_final/churn_5_95.sh 3 0 ICEBERG_SINGLE_THREAD

#./bench/paper_final/churn_50_50.sh 3 0 CLHT
#./bench/paper_final/churn_25_75.sh 3 0 CLHT
#./bench/paper_final/churn_5_95.sh 3 0 CLHT

./bench/paper_final/churn_50_50.sh 3 0 CUCKOO
#./bench/paper_final/churn_25_75.sh 3 0 CUCKOO
#./bench/paper_final/churn_5_95.sh 3 0 CUCKOO

./bench/paper_final/churn_50_50.sh 3 0 RHM
#./bench/paper_final/churn_25_75.sh 3 0 RHM
#./bench/paper_final/churn_5_95.sh 3 0 RHM

./bench/paper_final/churn_50_50.sh 3 0 TRHM
#./bench/paper_final/churn_25_75.sh 3 0 TRHM
#./bench/paper_final/churn_5_95.sh 3 0 TRHM

./bench/paper_final/churn_50_50.sh 3 0 GRHM
#./bench/paper_final/churn_25_75.sh 3 0 GRHM
#./bench/paper_final/churn_5_95.sh 3 0 GRHM

./bench/paper_final/churn_50_50.sh 3 0 GZHM_ADAPTIVE
#./bench/paper_final/churn_25_75.sh 3 0 GZHM_ADAPTIVE
#./bench/paper_final/churn_5_95.sh 3 0 GZHM_ADAPTIVE
