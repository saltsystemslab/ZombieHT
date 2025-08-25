# Zombie Hashing

Zombie Hashing is a linear probing hashing scheme that ensures consistent performance with high data locality even at high load factors. Linear probing hash tables use tombstones (deletion markers) to speed up hash table operations and mitigate primary clustering effects. However, tombstones require periodic redistribution, requiring a complete halt of operations and consequently inconsistent performance. Zombie Hashing redistributes tombstones within small windows, eliminating the need for periodic halts required in linear probing schemes. 

We implement Zombie Hashing in two variants, an ordered compact linear probing hash table([code](src/gqf.c)) and an unordered vectorized linear probing hash table ([code](external/abseil-cpp/absl/container/internal/raw_hash_set.h)). 

More details about the implementation and theoretical guarantees are in the paper.

## Quick Start

```bash
git clone git@github.com:saltsystemslab/grht
cd grht

./setup.sh

./run_throughput.sh # Runs all churn benchmark tests to measure throughput.
./run_latency.sh # Runs all churn benchmark tests to measure latency.

./plot_data.sh # Generates a report (./bench/report/main.pdf)
```

## Churn Experiments

The scripts `./run_throughput.sh` and `./run_latency.sh` will run all the experiments reported in the paper.

The experiments measure the throughput and latency on ‘churn-cycle’ experiments on the hash tables. 

In the setup phase of the experiment, the hash tables are filled to a chosen load factor. 

We then run multiple churn cycles on the hash table. Each churn cycle first deletes 5% of the keys randomly in the hash table. It then performs the same amount of insertions (again using random keys) in the hash table. The test then performs lookup operations on the hash table.

We run the experiments with the following configurations
95% load factor, 50-50 split between updates and lookups, 320 churn cycles
95% load factor, 5-95 split between updates and lookups
Ordered tables: 2000 churn cycles
Unordered tables: 320 cycles
85% load factor, 50-50 split between updates and lookups, 320 churn cycles 
85% load factor, 5-95 split between updates and lookups, 320 churn cycles
75% load factor, 50-50 split between updates and lookups, 320 churn cycles
75% load factor, 5-95 split between updates and lookups, 320 churn cycles

Throughput is calculated by measuring the time to complete a single churn cycle.

Each experiment for a hash table is run in its own process. Each experiment outputs a csv file capturing the throughput (or latency) metrics per churn cycle. We use python scripts to parse the data from each experiment and plot the graphs in Latex. For details on how to run these experiments, please refer to the README and the Reproducibility section below.


## Churn Benchmark Details

The [ChurnBenchmark](bench/hm_churn.cc) will 

1. Fill the hashmap to a load factor `I` with keys from a uniform distribution.
2. Conduct `C` churn cycles. Each Churn cycle will sequentially
  1. Delete `L` keys from the hashmap uniform randomly.
  2. Insert new `L` keys into the hashmap uniform randomly.
  3. Lookup `L` keys. Keys being looked up are a mixture of keys that are currently in hashmap, were deleted previously and random keys.

## Running a churn test

Use `./bench/paper_final/churn.sh` to run a single churn test. The ./run_throughput.sh and ./run_latency.sh scripts invoke this script for each experiment.

```bash
$ ./bench/paper_final/churn.sh thput ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}
$ ./bench/paper_final/churn.sh latency ${VARIANT} ${LOAD_FACTOR} ${UPDATE_PCT} ${THROUGHPUT_FREQ}
```

### Hashmap Variants

1. RHM: RobinHood Hashmap. Linear Probing with keys ordered in a run. Deletion will remove the item and shift items back.
2. TRHM: RobinHood Hashmap with Tombstones. The same as RHM, but deletion leaves a tombstone that can be used by future inserts.
3. GRHM: Graveyard Hashmap. Insertions and Deletions proceed in same manner as TRHM. A rebuild schedule will trigger that will redistribute tombstones across the hashmap.
4. GZHM\_DELETE, GZHM: ZombieHashmap. Insertions and Deletions proceed in the same manner as TRHM, but tombstones are redistributed in a local region (region depends on implementation variant).
5. ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED: Unordered vectorized Zombie Hashing.
6. ABSL: A [forked](https://github.com/saltsystemslab/abseil-cpp) version of [abseil-cpp](https://abseil.io/) that disables resizing. **SEE [ABSL VARIANTS README](https://github.com/saltsystemslab/abseil-cpp/blob/linear-probe/README.md) for ABSL VARIANTS.**
7. ICEBERG: [Iceberg HashTable](https://github.com/splatlab/iceberghashtable)
8. CLHT: CLHT HashTable.


### Using PERF

To use perf, make sure to use .`/bench/paper_final/churn_perf.sh` instead of the above script. It uses `-DCMAKE_BUILD_TYPE=RelWithDebInfo` and runs the 

This will create `perf.data` Will only store perf report of last run variant.

