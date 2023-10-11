# First flag is input level.
if [ $1 -eq 0 ]; then
  run_args="-k 16 -q 8 -v 0 -w 10 -l 200 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot=-DQF_BITS_PER_SLOT=8
elif [ $1 -eq 1 ]; then
  run_args="-k 38 -q 22 -v 0 -w 41943 -l 838860 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot=-DQF_BITS_PER_SLOT=16
elif [ $1 -eq 2 ]; then
  run_args="-k 59 -q 27 -v 0 -w 1342100 -l 26843500 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot=-DQF_BITS_PER_SLOT=32
elif [ $1 -eq 3 ]; then
  run_args="-k 64 -q 27 -v 0 -w 1342100 -l 26843500 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot=
else 
  echo "Specify input data level"
  exit
fi

# Second flag is workload (mixed for throughput, nomixed for latency)
if [ $2 -eq 0 ]; then
  churn_args="-c 250 -m 1"
  latency=""
elif [ $2 -eq 1 ]; then
  churn_args="-c 80 -m 0 -z 50"
  latency="_latency"
else 
  echo "Specify non-mixed mode or mixed mode"
  exit
fi

VARIANTS=("ABSL" "ICEBERG" "CLHT" "GZHM")

out_dir="sponge/$(date +%s)_gzhm_external${latency}_$1"
build_dir=${out_dir}/build
run_dir=${out_dir}/run
result_dir=${out_dir}/result

mkdir -p sponge
rm -rf ${build_dir}
rm -rf ${run_dir}
rm -rf ${result_dir}
mkdir -p ${build_dir}
mkdir -p ${run_dir}
mkdir -p ${result_dir}

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p ${build_dir}/$VARIANT
  cmake . -B${build_dir}/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT ${qf_bits_per_slot}
  cmake --build ${build_dir}/$VARIANT -j8
done

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p ${run_dir}/$VARIANT
  echo ./${build_dir}/$VARIANT/hm_churn $run_args $churn_args -d ${run_dir}/$VARIANT/
  numactl -N 0 -m 0 ./${build_dir}/$VARIANT/hm_churn $run_args $churn_args -d ${run_dir}/$VARIANT/
done

echo python3 ./bench/plot_graph.py ${run_dir}  ${result_dir}
python3 ./bench/plot_graph.py ${run_dir} ${result_dir}
