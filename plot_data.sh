#!/bin/bash
for dir in sponge_paper/*; do
    python3 ./bench/plot_graph.py $dir/gzhm_variants_thput/run $dir/gzhm_variants_thput/result
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
