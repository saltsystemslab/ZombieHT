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

### Workload

### Running the benchmark

### Evaluation graphs


## README TODOs

Last Updated: Sept 16, 2023

* Add citations.
* Overview is a placeholder for now.
