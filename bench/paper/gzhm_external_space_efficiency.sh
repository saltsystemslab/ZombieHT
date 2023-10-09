# First flag is input level.
if [ $1 -eq 0 ]; then
  run_args="-k 16 -q 8 -v 0 -w 10 -l 200 -s 0 -t 1 -g 50"
  qf_bits_per_slot=8
elif [ $1 -eq 1 ]; then
  run_args="-k 38 -q 22 -v 0 -w 41943 -l 838860 -s 0 -t 1 -g 50"
  qf_bits_per_slot=16
elif [ $1 -eq 2 ]; then
  run_args="-k 59 -q 27 -v 0 -w 1342100 -l 26843500 -s 0 -t 1 -g 50"
  qf_bits_per_slot=32
else 
  echo "Specify input data level"
  exit
fi

# Second flag is workload (mixed for throughput, nomixed for latency)
if [ $2 -eq 0 ]; then
  churn_args="-c 30 -m 1"
  latency=""
elif [ $2 -eq 1 ]; then
  churn_args="-c 80 -m 0 -z 50"
  latency="_latency"
else 
  echo "Specify non-mixed mode or mixed mode"
  exit
fi

VARIANTS=("ABSL" "ICEBERG" "GZHM" "CLHT")
LOAD_FACTOR=("35" "45" "55" "65" "75" "85" "95")

out_dir="sponge/$(date +%s)_gzhm_lf${latency}_$1"
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
  cmake . -B${build_dir}/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT -DQF_BITS_PER_SLOT=${qf_bits_per_slot} -DPTS=1.5
  cmake --build ${build_dir}/$VARIANT -j8
done

for VARIANT in "${VARIANTS[@]}"; do
for LF in "${LOAD_FACTOR[@]}"; do
  mkdir -p ${run_dir}/${VARIANT}_${LF}
  echo ./${build_dir}/$VARIANT/hm_churn $run_args $churn_args -d ${run_dir}/$VARIANT/
  numactl -N 0 -m 0 ./${build_dir}/${VARIANT}/hm_churn $run_args -i ${LF} $churn_args -d ${run_dir}/${VARIANT}_${LF}/
done
done

echo python3 ./bench/plot_graph.py ${run_dir} 
python3 ./bench/plot_graph.py ${run_dir} ${result_dir}
