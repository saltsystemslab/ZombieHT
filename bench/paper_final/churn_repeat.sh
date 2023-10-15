#!/bin/bash

# $1 - input level, 0 is 8 bits, 3 is 27 bits
# $2 - test type - throughput or latency. latency test is smaller.
# $3 - HM type.
# $4 - repeat cycles.

# First flag is input level.
if [ $1 -eq 0 ]; then
  run_args="-k 16 -q 8 -v 0 -w 10 -l 200 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot="-DQF_BITS_PER_SLOT=8"
elif [ $1 -eq 1 ]; then
  run_args="-k 38 -q 22 -v 0 -w 41943 -l 838860 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot="-DQF_BITS_PER_SLOT=16"
elif [ $1 -eq 2 ]; then
  run_args="-k 59 -q 27 -v 0 -w 1342100 -l 26843500 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot="-DQF_BITS_PER_SLOT=32"
elif [ $1 -eq 3 ]; then
  run_args="-k 64 -q 27 -v 0 -w 1342100 -l 26843500 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot=
elif [ $1 -eq 4 ]; then
  run_args="-k 64 -q 27 -v 0 -w 1342100 -l 26843500 -i 95 -s 0 -t 1 -g 50"
  qf_bits_per_slot=64
else 
  echo "Specify input data level"
  exit
fi

# Second flag is workload (mixed for throughput, nomixed for latency)
if [ $2 -eq 0 ]; then
  churn_args="-c 250 -m 0"
  latency=""
elif [ $2 -eq 1 ]; then
  churn_args="-c 100 -m 0 -z 50"
  latency="_latency"
else 
  echo "Specify non-mixed mode or mixed mode"
  exit
fi

out_dir="sponge/churn${latency}_$1_$4"
build_dir=${out_dir}/build
mkdir -p sponge
mkdir -p ${out_dir}
mkdir -p ${build_dir}

VARIANTS=($3)
for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p ${build_dir}/$VARIANT
  cmake . -B${build_dir}/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT
  cmake --build ${build_dir}/$VARIANT -j8
done


for i in {1..10}
do
echo $i

run_dir=${out_dir}/run
result_dir=${out_dir}/result

mkdir -p sponge
mkdir -p ${run_dir}
mkdir -p ${result_dir}

for VARIANT in "${VARIANTS[@]}"; do
  mkdir -p ${run_dir}/$VARIANT
  mkdir -p ${run_dir}/$VARIANT/$i
  echo ./${build_dir}/$VARIANT/hm_churn ${run_args} ${churn_args} -d ${run_dir}/$VARIANT/
  numactl -N 0 -m 0 ./${build_dir}/$VARIANT/hm_churn $run_args $churn_args -d ${run_dir}/$VARIANT/$i/
done

done

echo python3 ./bench/plot_graph.py ${run_dir} ${result_dir}
#python3 ./bench/plot_graph.py ${run_dir} ${result_dir}
