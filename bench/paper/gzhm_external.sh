#!/bin/bash
#run_args="-k 38 -q 22 -v 0 -c 150 -w 41943 -l 838860 -i 95 -s 1 -t 1 -m 1 -g 50 -z 50"
run_args="-k 59 -q 27 -v 0 -c 200 -w 1342100 -l 26843500 -i 95 -s 1 -t 1 -m 1 -g 50"

if [ -z "$1" ]; then
    out_dir="paper/gzhm_external"
else
    out_dir="$1"
fi

rm -rf $out_dir/*

mkdir -p build
VARIANTS=("GZHM" "ABSL" "ICEBERG")

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p build/$VARIANT
  echo  -p $out_dir/$VARIANT
  cmake . -Bbuild/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT -DQF_BITS_PER_SLOT=32
  cmake --build build/$VARIANT -j8
done

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p $out_dir/$VARIANT
  echo  -p $out_dir/$VARIANT
  echo ./build/$VARIANT/hm_churn $run_args -d $out_dir/$VARIANT
  numactl -N 0 -m 0 ./build/$VARIANT/hm_churn $run_args -d $out_dir/$VARIANT/
done

python3 ./bench/plot_graph.py $out_dir