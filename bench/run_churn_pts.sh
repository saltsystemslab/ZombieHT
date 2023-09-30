run_args="-k 38 -q 22 -v 0 -c 500 -l 10000 -i 95 -s 1 -t 8"
if [ -z "$1" ]; then
    out_dir="bench_run"
else
    out_dir="$1"
fi

rm -rf $out_dir/*

mkdir -p build/PTS_1
cmake . -Bbuild/PTS_1 -DCMAKE_BUILD_TYPE=Release -DVARIANT=GZHM_DELETE -DPTS=1.0
cmake --build build/PTS_1 -j8

mkdir -p build/PTS_2
cmake . -Bbuild/PTS_2 -DCMAKE_BUILD_TYPE=Release -DVARIANT=GZHM_DELETE -DPTS=2.0
cmake --build build/PTS_2 -j8

mkdir -p $out_dir/PTS_1
echo ./build/PTS_1/hm_churn $run_args -d $out_dir/PTS_1
numactl -N 0 -m 0 ./build/PTS_1/hm_churn $run_args -d $out_dir/PTS_1/

mkdir -p $out_dir/PTS_2
echo ./build/PTS_2/hm_churn $run_args -d $out_dir/PTS_2
numactl -N 0 -m 0 ./build/PTS_2/hm_churn $run_args -d $out_dir/PTS_2/

python3 ./bench/plot_graph.py $out_dir
