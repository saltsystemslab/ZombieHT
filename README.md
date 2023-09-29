# ZombieHashmap

ZombieHashmap is a hashmap that redistributes tombstones in a deamortized schedule. 

Conventional wisdom dictates if using tombstones to mark deletions, they must be cleared completely. This is usually done in a stop-the-world fashion.

[Bender, M. A., Kuszmaul, B. C., &amp; Kuszmaul, W. (2021, July 2). Linear probing revisited: Tombstones mark the death of primary clustering.](https://arxiv.org/abs/2107.01250) 
shows that spreading the tombstones apart is more optimal at steady load factors. 

ZombieHashmap spreads tombstones on the back of hashmap operations, thus not requiring to freeze the hashmap to spread tombstones.

## Churn Benchmark

The [ChurnBenchmark](bench/hm_churn.cc) will 

1. Fill the hashmap to a load factor `I` with keys from a uniform distribution.
2. Conduct `C` churn cycles. Each Churn cycle will sequentially
  1. Delete `L` keys from the hashmap uniform randomly.
  2. Insert new `L` keys into the hashmap uniform randomly.
  3. Lookup `L` keys. Keys being looked up are a mixture of keys that are currently in hashmap, were deleted previously and random keys.

### Hashmap Variants

1. RHM: RobinHood Hashmap. Linear Probing with keys ordered in a run. Deletion will remove the item and shift items back.
2. TRHM: RobinHood Hashmap with Tombstones. The same as RHM, but deletion leaves a tombstone that can be used by future inserts.
3. GRHM: Graveyard Hashmap. Insertions and Deletions proceed in same manner as TRHM. A rebuild schedule will trigger that will redistribute tombstones across the hashmap.
4. GZHM\_DELETE, GZHM: ZombieHashmap. Insertions and Deletions proceed in the same manner as TRHM, but tombstones are redistributed in a local region (region depends on implementation variant).

External Variants we compare against.

1. ABSL: A [forked](https://github.com/saltsystemslab/abseil-cpp) version of [abseil-cpp](https://abseil.io/) that disables resizing.
2. ICEBERG: [Iceberg HashTable](https://github.com/splatlab/iceberghashtable)

### Running the benchmark

#### TL;DR version

Run the below command and see plots in `./bench_result`

```bash
./scripts/make_dependencies.sh
./bench/run_cmake_churn.sh
```

#### Building and Running


```bash
mkdir build
cd build
cmake ../ --DCMAKE_BUILD_TYPE=Release -DVARIANT=GZHM_DELETE -DPTS=1.5
cmake --build .
```

This will build `./hm_churn` in your build directory.

```bash
$ ./build/ABSL/hm_churn -h
./build/ABSL/hm_churn: invalid option -- 'h'
Unknown option
./build/ABSL/hm_churn [OPTIONS]
Options are:
  -d dir                [ Output Directory. Default bench_run ]
  -k keybits            [ Size of key in bits. ]
  -q quotient_bits      [ Size of quotient in bits. ]
  -v value_keybits      [ Size of value in bits. ]
  -i initial load       [ Initial Load Factor[0-100]. Default 94 ]
  -c churn cycles       [ Number of churn cycles.  Default 10 ]
  -l churn length       [ Number of insert, delete operations per churn cycle ]
  -r record             [ Whether to record. If 1 will record to -f. Use test_runner to replay or check test case.]
  -f record/replay file [ File to record to. Default test_case.txt ]
  -p npoints            [ number of points on the graph for load phase.  Default 20]
  -t throughput buckets [number of points to collect per churn phase op.  Default 4]
  -g latency sample rate[ churn op latency sampling rate.  Default 1000]
  -s silent             [ Default 1. Use 0 for verbose mode
]
$
```

The below will dump metrics to a `bench_result` directory.

```bash
$ mkdir bench_result
$ ./build/ABSL/hm_churn -k 38 -q 22 -v 0 -c 6 -l 10000 -i 95 -s 1 -t 8 -d bench_result/                                               
max_load_factor: 0.950000
overall load insert throughput (ops/microsec): 16.882290
```

### Evaluation graphs

`./bench/plot_graph` will generate graphs for throughput and latency. It expects all variant results to be under a single directory.

See this [script](./bench/run_cmake_run.sh) for details.

## README TODOs

Last Updated: Sept 16, 2023

* Add citations.
* Overview is a placeholder for now.
* Explain what -q is and quotienting.
