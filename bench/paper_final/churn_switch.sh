MIXED_WORKLOAD=$1 # 0: measure throughput 1: measure latency on mixed workload
VARIANT=$2 # Refer to CMake file for list of variants
INIT_LF=$3 #Initial Load Factor
UPDATE_PCT=$4 # $4 + $5 must be 100, but we don't check


DIR="sponge/large-test_init-lf-${INIT_LF}_update-pct-${UPDATE_PCT}"
QF_ARGS="-k 64 -q 27 -v 0"
CYCLES=20

case $UPDATE_PCT in
    5)
    UPDATES=83886
    LOOKUPS=3187670
    ;;
    50)
    UPDATES=838860
    LOOKUPS=1677721
    ;;
    *)
    echo "Invalid update size (5, 25, 50) - aborting"
    exit 1
esac

if [ $MIXED_WORKLOAD == "thput" ]; then
  churn_args="-c ${CYCLES} -m 0"
  latency=""
elif [ $MIXED_WORKLOAD == "latency" ]; then
  churn_args="-c ${CYCLES} -m 1"
  latency="latency"
else 
  echo "-m Specify measurement 'thput' or 'latency'"
  exit
fi

mkdir -p $DIR
run_args="${QF_ARGS} -w ${UPDATES} -l ${LOOKUPS} -i ${INIT_LF} -s 0 -t 1 -g 50 ${churn_args}"
echo $run_args



out_dir="$DIR/gzhm_variants${latency}_$1"
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
numactl -N 0 -m 0 ./${build_dir}/$VARIANT/hm_churn $run_args $churn_args -d ${run_dir}/$VARIANT/

echo python3 ./bench/plot_graph.py ${run_dir} ${result_dir}
python3 ./bench/plot_graph.py ${run_dir} ${result_dir}
