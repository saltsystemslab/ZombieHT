run_args="-k 38 -q 22 -v 0 -c 100 -l 10000 -i 95 -s 1 -t 8"

if [ -z "$1" ]; then
    out_dir="bench_run"
else
    out_dir="$1"
fi

rm -rf $out_dir/*

mkdir -p build
VARIANTS=("GZHM" "RHM" "GZHM_DELETE" "TRHM" "GRHM")

for VARIANT in "${VARIANTS[@]}"; do
  rm -rf build/$VARIANT
  mkdir -p build/$VARIANT
  cmake . -Bbuild/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT
  cmake --build build/$VARIANT
done

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p $out_dir/$VARIANT
  echo ./build/$VARIANT/hm_churn $run_args -d $out_dir/$VARIANT
  ./build/$VARIANT/hm_churn $run_args -d $out_dir/$VARIANT/
done

python3 ./bench/plot_graph.py $out_dir
