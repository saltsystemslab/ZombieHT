run_args="-k 38 -q 22 -v 0 -c 500 -w 10000 -l 200000 -i 80 -s 1 -t 1 -m 1 -g 50"

if [ -z "$1" ]; then
    out_dir="bench_run_small_mixed"
else
    out_dir="$1"
fi

rm -rf $out_dir/*

mkdir -p build
#VARIANTS=("TRHM" "RHM" "GZHM" "GZHM_DELETE" "GRHM" "ICEBERG" "ABSL" "CLHT")
VARIANTS=("ICEBERG")

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p build/$VARIANT
  cmake . -Bbuild/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT
  cmake --build build/$VARIANT -j8
done

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p $out_dir/$VARIANT
  echo ./build/$VARIANT/hm_churn $run_args -d $out_dir/$VARIANT/
  timeout 7m numactl -N 0 -m 0 ./build/$VARIANT/hm_churn $run_args -d $out_dir/$VARIANT/
done

python3 ./bench/plot_graph.py $out_dir
