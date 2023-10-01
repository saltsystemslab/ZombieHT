run_args="-k 59 -q 27 -v 0 -c 150 -w 1342100 -l 26843500 -i 95 -s 1 -t 1 -m 0 -g 50"

if [ -z "$1" ]; then
    out_dir="bench_run_nomix"
else
    out_dir="$1"
fi

rm -rf $out_dir/*

mkdir -p build
VARIANTS=("GZHM" "ABSL" "CLHT" "ICEBERG" "GZHM_DELETE")

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p build/$VARIANT
  cmake . -Bbuild/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT -DQF_BITS_PER_SLOT=32
  cmake --build build/$VARIANT -j8
done

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p $out_dir/$VARIANT
  echo ./build/$VARIANT/hm_churn $run_args -d $out_dir/$VARIANT
  numactl -N 0 -m 0 ./build/$VARIANT/hm_churn $run_args -d $out_dir/$VARIANT/
done

python3 ./bench/plot_graph.py $out_dir
