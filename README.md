# Zombie Hashing

Zombie Hashing is a linear probing hashing scheme that ensures consistent performance with high data locality even at high load factors. Linear probing hash tables use tombstones (deletion markers) to speed up hash table operations and mitigate primary clustering effects. However, tombstones require periodic redistribution, requiring a complete halt of operations and consequently inconsistent performance. Zombie Hashing redistributes tombstones within small windows, eliminating the need for periodic halts required in linear probing schemes. 

We implement Zombie Hashing in two variants, an ordered compact linear probing hash table([code](src/gqf.c)) and an unordered vectorized linear probing hash table ([code](external/abseil-cpp/absl/container/internal/raw_hash_set.h)). 

More details about the implementation and theoretical guarantees are in the paper.

## Quick Start

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
cd external/clht
make dependencies clht_lb
cd ../../
./run_throughput.sh # Runs all churn benchmark tests to measure throughput.
./run_latency.sh # Runs all churn benchmark tests to measure latency.
```

## Churn Benchmark Details

The [ChurnBenchmark](bench/hm_churn.cc) will 

1. Fill the hashmap to a load factor `I` with keys from a uniform distribution.
2. Conduct `C` churn cycles. Each Churn cycle will sequentially
  1. Delete `L` keys from the hashmap uniform randomly.
  2. Insert new `L` keys into the hashmap uniform randomly.
  3. Lookup `L` keys. Keys being looked up are a mixture of keys that are currently in hashmap, were deleted previously and random keys.

## Running a churn test

Using the below script is the to run the churn test. `bench/paper_final/churn.sh` contains a workload with a churn cycle containing 5% inserts, 5% deletes and 20% lookups. Other scripts in `bench/paper_final` contain varying read/write ratio churn cycle benchmarks.

```bash
$ ./bench/paper_final/churn.sh <TEST_CASE> <MEASURE_LATENCY> <HM_VARIANT>
```

TEST_CASE should be one of (1,2,3)
- 1 : key size 38 bits, quotient 22 bits
- 2 : key size 59 bits, quotient 27 bits
- 3: key size 64 bits, quotient 27 bits

Quotient only matters for quotient filter variants.

MEASURE_LATENCY should be one of (0, 1)
* 0: Don't measure latency, only throughput
* 1: Sample latency

VARIANTS: Refer to CMakeLists.txt for list of hashmap variants

### Hashmap Variants

1. RHM: RobinHood Hashmap. Linear Probing with keys ordered in a run. Deletion will remove the item and shift items back.
2. TRHM: RobinHood Hashmap with Tombstones. The same as RHM, but deletion leaves a tombstone that can be used by future inserts.
3. GRHM: Graveyard Hashmap. Insertions and Deletions proceed in same manner as TRHM. A rebuild schedule will trigger that will redistribute tombstones across the hashmap.
4. GZHM\_DELETE, GZHM: ZombieHashmap. Insertions and Deletions proceed in the same manner as TRHM, but tombstones are redistributed in a local region (region depends on implementation variant).
5. ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED: Unordered vectorized Zombie Hashing.
6. ABSL: A [forked](https://github.com/saltsystemslab/abseil-cpp) version of [abseil-cpp](https://abseil.io/) that disables resizing. **SEE [ABSL VARIANTS README](https://github.com/saltsystemslab/abseil-cpp/blob/linear-probe/README.md) for ABSL VARIANTS.**
7. ICEBERG: [Iceberg HashTable](https://github.com/splatlab/iceberghashtable)
8. CLHT: CLHT HashTable.


### Example commands

```bash
# Example commands
$ ./bench/paper_final/churn.sh 3 0 GZHM
$ ./bench/paper_final/churn.sh 3 0 ABSL
$  python3 ./bench/plot_graph.py sponge/gzhm_variants_3/run sponge/gzhm_variants_1/result
```

The above script will
* Create a directory `sponge/gzhm_variants_3`
	* Here `3` refers to the test case.
* For each variant a build will be generated in `./sponge/gzhm_variants_3/build/<VARIANT>`
	* The churn test binary is named `hm_churn`
* Will run a churn test for 220 cycles.
	* Refer to `src/hm_churn.cc` for the flags set in `./bench/paper_final/churn.sh`
* Generates test results for each variant to `./sponge/gzhm_variants_3/run/<VARIANT>`
* Finally `./bench/plot_graph.py` will generate graphs for all variants in `./bench/gzhm_variants_3/run/`

### Using PERF

To use perf, make sure to use .`/bench/paper_final/churn_perf.sh` instead of the above script. It uses `-DCMAKE_BUILD_TYPE=RelWithDebInfo` and runs the 

This will create `perf.data` Will only store perf report of last run variant.

