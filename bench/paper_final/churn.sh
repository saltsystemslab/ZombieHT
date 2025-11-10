MIXED_WORKLOAD=$1 # 0: measure throughput 1: measure latency on mixed workload
VARIANT=$2 # Refer to CMake file for list of variants
INIT_LF=$3 #Initial Load Factor
UPDATE_PCT=$4 # $4 + $5 must be 100, but we don't check
THROUGHPUT_FREQ=$5 # number of points to collect per churn cycle.
#Note this just divides the cycles into smaller churn cycles, 
CHURN_CYCLES=$6 # Number of churn cyles to run in total

echo ${CHURN_CYCLES}
echo ${THROUGHPUT_FREQ}

DIR="sponge_paper/large-test_init-lf-${INIT_LF}_update-pct-${UPDATE_PCT}_thput-freq_${THROUGHPUT_FREQ}"
QF_ARGS="-k 64 -q 27 -v 0"
CYCLES=$((${CHURN_CYCLES} * ${THROUGHPUT_FREQ}))
echo ${CYCLES}

case $UPDATE_PCT in
    5)
    UPDATES=$((167772 / ${THROUGHPUT_FREQ}))
    LOOKUPS=$((6375340 / ${THROUGHPUT_FREQ}))
    ;;
    50)
      UPDATES=$((1677721 / ${THROUGHPUT_FREQ}))
      LOOKUPS=$((3355442 / ${THROUGHPUT_FREQ}))
    ;;
    *)
    echo "Invalid update size (5, or 50) - aborting"
    exit 1
esac

if [ $MIXED_WORKLOAD == "thput" ]; then
  churn_args="-c ${CYCLES} -m 0 -g 0"
  latency=""
elif [ $MIXED_WORKLOAD == "latency" ]; then
  churn_args="-c ${CYCLES} -z 50 -g 50"
  latency="latency"
else 
  echo "-m Specify measurement 'thput' or 'latency'"
  exit
fi

mkdir -p $DIR
run_args="${QF_ARGS} -w ${UPDATES} -l ${LOOKUPS} -i ${INIT_LF} -s 0 -t 1 ${churn_args}"
echo $run_args


out_dir="$DIR/gzhm_variants_$1"
build_dir=${out_dir}/build
run_dir=${out_dir}/run
result_dir=${out_dir}/result

mkdir -p ${DIR}
mkdir -p ${out_dir}
mkdir -p ${run_dir}
mkdir -p ${result_dir}

mkdir -p ${build_dir}/$VARIANT
cmake . -B${build_dir}/$VARIANT -DCMAKE_BUILD_TYPE=Release -DVARIANT=$VARIANT
cmake --build ${build_dir}/$VARIANT -j8

rm -rf ${run_dir}/$VARIANT
rm -rf ${result_dir}/$VARIANT
mkdir -p ${run_dir}/$VARIANT
echo ./${build_dir}/$VARIANT/hm_churn ${run_args} ${churn_args} -d ${run_dir}/$VARIANT/
./${build_dir}/$VARIANT/hm_churn $run_args $churn_args -d ${run_dir}/$VARIANT/

#echo python3 ./bench/plot_graph.py ${run_dir} ${result_dir}
#python3 ./bench/plot_graph.py ${run_dir} ${result_dir}
