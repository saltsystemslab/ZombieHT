# First flag is input level.
DIR=sponge_50_50
UPDATES=13421768
LOOKUPS=26843544
CYCLES=320

if [ $1 -eq 3 ]; then
  run_args="-k 64 -q 27 -v 0 -w $UPDATES -l $LOOKUPS -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot=
else 
  echo "Specify input data level"
  exit
fi

# Second flag is workload (mixed for throughput, nomixed for latency)
if [ $2 -eq 0 ]; then
  churn_args="-c $CYCLES -m 0"
  latency=""
elif [ $2 -eq 1 ]; then
  echo "Latency mode not setup"
  exit
else 
  echo "Specify non-mixed mode or mixed mode"
  exit
fi

VARIANTS=($3)

out_dir="$DIR/gzhm_variants${latency}_$1"
build_dir=${out_dir}/build
run_dir=${out_dir}/run
result_dir=${out_dir}/result

mkdir -p ${DIR}
mkdir -p ${out_dir}
mkdir -p ${run_dir}
mkdir -p ${result_dir}

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p ${build_dir}/$VARIANT
  cmake . -B${build_dir}/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT
  cmake --build ${build_dir}/$VARIANT -j8
done

for VARIANT in "${VARIANTS[@]}"; do
  rm -rf ${run_dir}/$VARIANT
  rm -rf ${result_dir}/$VARIANT
  mkdir -p ${run_dir}/$VARIANT
  echo ./${build_dir}/$VARIANT/hm_churn ${run_args} ${churn_args} -d ${run_dir}/$VARIANT/
  numactl -N 0 -m 0 ./${build_dir}/$VARIANT/hm_churn $run_args $churn_args -d ${run_dir}/$VARIANT/
done

echo python3 ./bench/plot_graph.py ${run_dir} ${result_dir}
python3 ./bench/plot_graph.py ${run_dir} ${result_dir}
