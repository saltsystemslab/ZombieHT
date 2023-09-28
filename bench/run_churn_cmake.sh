run_args="-k 38 -q 22 -v 0 -c 100 -w 10000 -l 200000 -i 95 -s 1 -t 8"
# run_args="-k 43 -q 27 -v 0 -c 100 -w 40000 -e 40000 -l 40000 -i 95 -s 1"
# run_args="-k 43 -q 27 -v 0 -c 200 -w 400000 -e 400000 -l 400000 -i 95 -s 1 -t 4"

if [ -z "$1" ]; then
    out_dir="bench_run"
else
    out_dir="$1"
fi

rm -rf $out_dir/*

mkdir -p build
VARIANTS=("ABSL" "TRHM" "RHM" "GZHM" "GZHM_DELETE" "GRHM")

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p build/$VARIANT
  cmake . -Bbuild/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT
  cmake --build build/$VARIANT -j8
done

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p $out_dir/$VARIANT
  echo ./build/$VARIANT/hm_churn $run_args -d $out_dir/$VARIANT
  numactl -N 0 -m 0 ./build/$VARIANT/hm_churn $run_args -d $out_dir/$VARIANT/
done

python3 ./bench/plot_graph.py $out_dir
