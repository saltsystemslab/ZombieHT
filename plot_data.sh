#!/bin/bash

# Checks to not fail if other baselines do not exist
files=(
    "ICEBERG_SINGLE_THREAD_OVERALL_throughput.csv"
    "CLHT_OVERALL_throughput.csv"
    "CUCKOO_OVERALL_throughput.csv"
)

for dir in sponge_paper/*; do
    python3 ./bench/plot_graph.py $dir/gzhm_variants_thput/run $dir/gzhm_variants_thput/result

  for file in "${files[@]}"; do
    mkdir -p "$dir/gzhm_variants_thput/result/csv/"
    if [ ! -f "$dir/gzhm_variants_thput/result/csv/$file" ]; then
      echo "$dir/gzhm_variants_thput/result/csv/$file does not exist! Creating an empty result for it."
      touch "$dir/gzhm_variants_thput/result/csv/$file"
    fi;
  done
done

for dir in sponge_paper/*; do
    python3 ./bench/plot_graph.py $dir/gzhm_variants_latency/run $dir/gzhm_variants_latency/result
done

cd bench/report
ln -sf ../../sponge_paper .
make brute-nonstop
make brute-nonstop
cp main.pdf ../../
cd ../../
