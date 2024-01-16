#!/bin/bash

#./bench/paper/gzhm_external.sh 2 0
#./bench/paper/gzhm_external.sh 2 1
# ./bench/paper/gzhm_variants.sh 2 0
#./bench/paper/gzhm_variants.sh 2 1
#./bench/paper/gzhm_pts_study.sh 2 0
#./bench/paper/gzhm_delete_study.sh 2 0

./bench/paper_final/churn.sh 1 0 RHM
./bench/paper_final/churn.sh 1 0 TRHM
./bench/paper_final/churn.sh 1 0 GRHM
./bench/paper_final/churn.sh 1 0 GZHM
./bench/paper_final/churn.sh 1 0 ABSL
