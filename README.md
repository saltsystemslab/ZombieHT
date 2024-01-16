# Setup

```bash
git clone git@github.com:saltsystemslab/grht
cd grht
git submodule update --init --recursive
cd external/abseil-cpp
git checkout linear-probe
cd ../../
cd external/libcuckoo
git checkout get_size
cd ../../
./run_all.sh # Runs all throughput tests.
```


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

```
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
```

### Hashmap Variants

1. RHM: RobinHood Hashmap. Linear Probing with keys ordered in a run. Deletion will remove the item and shift items back.
2. TRHM: RobinHood Hashmap with Tombstones. The same as RHM, but deletion leaves a tombstone that can be used by future inserts.
3. GRHM: Graveyard Hashmap. Insertions and Deletions proceed in same manner as TRHM. A rebuild schedule will trigger that will redistribute tombstones across the hashmap.
4. GZHM\_DELETE, GZHM: ZombieHashmap. Insertions and Deletions proceed in the same manner as TRHM, but tombstones are redistributed in a local region (region depends on implementation variant).

External Variants we compare against.

1. ABSL: A [forked](https://github.com/saltsystemslab/abseil-cpp) version of [abseil-cpp](https://abseil.io/) that disables resizing. **SEE [ABSL VARIANTS README](https://github.com/saltsystemslab/abseil-cpp/blob/linear-probe/README.md) for ABSL VARIANTS.**
2. ICEBERG: [Iceberg HashTable](https://github.com/splatlab/iceberghashtable)
3. CLHT: CLHT HashTable.

## Churn test Script

Using the below script is the quickest way to get started.

```bash
$ ./bench/paper_final/churn.sh <TEST_CASE> <MEASURE_LATENCY> <HM_VARIANT>
```

TEST_CASE should be one of (1,2,3) (explained below)
- 1 : key size 38 bits, quotient 22 bits
- 2 : key size 59 bits, quotient 27 bits
- 3: key size 64 bits, quotient 27 bits

Quotient only matters for quotient filter variants.

MEASURE_LATENCY should be one of (0, 1)
* 0: Don't measure latency, only throughput
* 1: Sample latency

VARIANTS: Refer to CMakeLists.txt for list of hashmap variants

### Example commands

```bash
# Example commands
$ ./bench/paper_final/churn.sh 1 0 GZHM
$ ./bench/paper_final/churn.sh 1 0 ABSL
$  python3 ./bench/plot_graph.py sponge/gzhm_variants_3/run sponge/gzhm_variants_1/result
```

The above script will
* Create a directory `sponge/gzhm_variants_1`
	* Here `1` refers to the test case.
* For each variant a build will be generated in `./sponge/gzhm_variants_1/build/<VARIANT>`
	* The churn test binary is named `hm_churn`
* Will run a churn test for 220 cycles.
	* Refer to `src/hm_churn.cc` for the flags set in `./bench/paper_final/churn.sh`
* Generates test results for each variant to `./sponge/gzhm_variants_1/run/<VARIANT>`
* Finally `./bench/plot_graph.py` will generate graphs for all variants in `./bench/gzhm_variants_1/run/`

### Using PERF

To use perf, make sure to use .`/bench/paper_final/churn_perf.sh` instead of the above script. It uses `-DCMAKE_BUILD_TYPE=RelWithDebInfo` and runs the 

This will create `perf.data` Will only store perf report of last run variant.

