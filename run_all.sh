#!/bin/bash

#./bench/paper/gzhm_external.sh 2 0
#./bench/paper/gzhm_external.sh 2 1
# ./bench/paper/gzhm_variants.sh 2 0
#./bench/paper/gzhm_variants.sh 2 1
#./bench/paper/gzhm_pts_study.sh 2 0
#./bench/paper/gzhm_delete_study.sh 2 0

./bench/paper_final/churn.sh 1 0 ABSL
./bench/paper_final/churn.sh 1 0 ABSL_LINEAR
./bench/paper_final/churn.sh 1 0 ABSL_GRAVEYARD
./bench/paper_final/churn.sh 1 0 ABSL_LINEAR_REHASH_CLUSTER
./bench/paper_final/churn.sh 1 0 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED
./bench/paper_final/churn.sh 1 0 ABSL_LINEAR_REHASH_CLUSTER_GRAVEYARD_DEAMORTIZED

./bench/paper_final/churn.sh 3 0 ABSL
./bench/paper_final/churn.sh 3 0 ABSL_LINEAR
./bench/paper_final/churn.sh 3 0 ABSL_GRAVEYARD
./bench/paper_final/churn.sh 3 0 ABSL_LINEAR_REHASH_CLUSTER
./bench/paper_final/churn.sh 3 0 ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED
./bench/paper_final/churn.sh 3 0 ABSL_LINEAR_REHASH_CLUSTER_GRAVEYARD_DEAMORTIZED