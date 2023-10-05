# First flag is input level.
if [ $1 -eq 0 ]; then
  run_args="-k 16 -q 8 -v 0 -w 10 -l 200 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot=8
elif [ $1 -eq 1 ]; then
  run_args="-k 38 -q 22 -v 0 -w 41943 -l 838860 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot=16
elif [ $1 -eq 2 ]; then
  run_args="-k 59 -q 27 -v 0 -w 1342100 -l 26843500 -i 95 -s 1 -t 1 -g 50"
  qf_bits_per_slot=32
fi

# Second flag is workload (mixed for throughput, nomixed for latency)
if [ $2 -eq 0 ]; then
  churn_args="-c 200 -m 1"
  latency=""
elif [ $2 -eq 1 ]; then
  churn_args="-c 50 -m 0 -z 50"
  latency="_latency"
fi

out_dir="paper/gzhm_external$latency"
rm -rf $out_dir
mkdir -p build
VARIANTS=("ABSL" "ICEBERG" "GZHM")

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p build/$VARIANT
  cmake . -Bbuild/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT -DQF_BITS_PER_SLOT=${qf_bits_per_slot}
  cmake --build build/$VARIANT -j8
done

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p $out_dir/$VARIANT
  echo  -p $out_dir/$VARIANT
  echo ./build/$VARIANT/hm_churn $run_args $churn_args -d $out_dir/$VARIANT/
  numactl -N 0 -m 0 ./build/$VARIANT/hm_churn $run_args $churn_args -d $out_dir/$VARIANT/
done

echo python3 ./bench/plot_graph.py $out_dir
python3 ./bench/plot_graph.py $out_dir
