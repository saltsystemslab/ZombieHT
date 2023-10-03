/*
 * ============================================================================
 *
 *        Authors:  Prashant Pandey <ppandey@cs.stonybrook.edu>
 *                  Rob Johnson <robj@vmware.com>
 *
 * ============================================================================
 */

#include <assert.h>
#include <iostream>
#include <map>
#include <math.h>
#include <openssl/rand.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <iostream>
#include <chrono>
using namespace std;
using namespace std::chrono;

#include "hm_op.h"
#include "hm_wrapper.h"

#define MAX_VALUE(nbits) ((1ULL << (nbits)) - 1)
#define BITMASK(nbits) ((nbits) == 64 ? 0xffffffffffffffff : MAX_VALUE(nbits))

int key_bits = 16;
int quotient_bits = 8;
int value_bits = 8;
int initial_load_factor = 95;
int nchurns = 10;
int nchurn_insert_ops = 500;
int nchurn_delete_ops = 500;
int nchurn_lookup_ops = 500;
int npoints = 50;
int should_record = 0;
int churn_latency_bucket_size = 10;
int churn_thrput_resolution = 4;
int mixed_workload = 0;
int log_commit_freq = 10; // Commit results every commit_freq cycles.
int churn_window_for_latency = 0;
std::string record_file = "test_case.txt";
std::string dir = "./bench_run/";
uint64_t num_slots = 0;
uint64_t num_initial_load_keys = 0;
bool is_silent = true;
FILE *LOG;

struct HmMetadataMeasure {
  int churn_cycle;
  time_point<high_resolution_clock> ts;
  uint64_t num_occupied;
  uint64_t num_tombstones;
};

struct ThrputMeasure {
  int churn_cycle;
  std::string operation;
  time_point<high_resolution_clock> start_ts;
  time_point<high_resolution_clock> end_ts;
  uint64_t num_ops;
  double thrput() {
    return 1.0 * num_ops / ((end_ts - start_ts).count());
  }
};

struct LatencyMeasure {
  std::string op_name;
  uint64_t nanoseconds;
};

void write_load_thrput_to_file(time_point<high_resolution_clock> *ts, uint64_t npoints,
                          std::string filename, uint64_t num_ops) {
  FILE *fp = fopen(filename.c_str(), "a");
  fprintf(fp, "x_0    y_0\n");
  for (uint64_t exp = 0; exp < 2 * npoints; exp += 2) {
    fprintf(fp, "%lu", exp);
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(ts[exp+1] - ts[exp]);
    auto nanoseconds = duration.count();
    if (nanoseconds == 0)
      fprintf(fp, " %f", 0.);
    else
      fprintf(fp, " %f",
            ((1.0 * num_ops / npoints) / nanoseconds));
    fprintf(fp, "\n");
  }
  fclose(fp);
}

void write_churn_thrput_by_phase_to_file(
    std::vector<ThrputMeasure> &measures,
    time_point<high_resolution_clock> test_begin,
    bool write_headers,
    std::string filename) {
  FILE *fp = fopen(filename.c_str(), "a");
  if(write_headers) {
    printf("Writing Log\n");
    fprintf(fp, "churn_cycle  ts  duration   num_ops  op\n");
  }
  for (auto measure: measures) {
      fprintf(fp, "%d %lu %lu %lu %s\n", 
        measure.churn_cycle,  
        (measure.end_ts- test_begin).count(),  // ts
        (measure.end_ts - measure.start_ts).count(),  //duration
        measure.num_ops,
        measure.operation.c_str());
  }
  fclose(fp);
  measures.clear();
}

void write_churn_metadata_to_file(
    std::vector<HmMetadataMeasure> &measures,
    time_point<high_resolution_clock> test_begin,
    bool write_headers,
    std::string filename) {
  FILE *fp = fopen(filename.c_str(), "a");
  if(write_headers) fprintf(fp, "churn_cycle  ts  occupied tombstones\n");
  for (auto measure: measures) {
      fprintf(fp, "%d %lu %lu %lu\n", measure.churn_cycle, (measure.ts- test_begin).count(), measure.num_occupied,  measure.num_tombstones);
  }
  fclose(fp);
  measures.clear();
}

void write_churn_latency_by_phase_to_file(
    std::vector<LatencyMeasure> &measures,
    bool write_headers,
    std::string filename) {
  FILE *fp = fopen(filename.c_str(), "a");
  if (write_headers) fprintf(fp, "op    latency\n");
  for (auto measure: measures) {
      fprintf(fp, "%s %lu\n", measure.op_name.c_str(), measure.nanoseconds);
  }
  fclose(fp);
  measures.clear();
}

void usage(char *name) {
  printf(
      "%s [OPTIONS]\n"
      "Options are:\n"
      "  -d dir                [ Output Directory. Default bench_run ]\n"
      "  -k keybits            [ Size of key in bits. ]\n"
      "  -q quotient_bits      [ Size of quotient in bits. ]\n"
      "  -v value_keybits      [ Size of value in bits. ]\n"
      "  -i initial load       [ Initial Load Factor[0-100]. Default 94 ]\n"
      "  -c churn cycles       [ Number of churn cycles.  Default 10 ]\n"
      "  -l churn length       [ Number of lookup operations per churn ]\n"
      "cycle ]\n"
      "  -w churn length       [ Number of insert/delete operations per churn.]\n"
      "  -r record             [ Whether to record. If 1 will record to -f. Use test_runner to replay or check test case.]\n"
      "  -f record/replay file [ File to record to. Default test_case.txt ]\n"
      "  -p npoints            [ number of points on the graph for load phase.  Default 20]\n"
      "  -t throughput buckets [number of points to collect per churn phase op.  Default 4]\n"
      "  -g latency bucket size[ churn op latency bucket size.  Default 10]\n"
      "  -m Mixed workload     [ Shuffle operations in a churn cycle. ]\n"
      "  -s silent             [ Default 1. Use 0 for verbose mode] \n"
      "  -z latency            [ Use 0 for verbose mode]\n"
      "]\n",
      name);
}

void parseArgs(int argc, char **argv) {
  /* Argument parsing */
  int opt;
  char *term;
  int nchurn_ops;

  while ((opt = getopt(argc, argv, "d:k:q:v:i:c:w:l:f:p:r:s:g:t:m:z:")) != -1) {
    switch (opt) {
		case 'd':
				dir = std::string(optarg);
				break;
    case 'k':
      key_bits = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -k must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'q':
      quotient_bits = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -q must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'v':
      value_bits = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -v must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'i':
      initial_load_factor = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -i must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'c':
      nchurns = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -c must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'l':
      nchurn_lookup_ops = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -l must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'm':
      mixed_workload = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -m must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'w':
      nchurn_insert_ops = strtol(optarg, &term, 10);
      nchurn_delete_ops = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -w must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'p':
      npoints = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -p must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'r':
      should_record = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -r must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 's':
      is_silent = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -r must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'z':
      churn_window_for_latency = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -z must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;

    case 't':
      churn_thrput_resolution = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -t must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'f':
      record_file = std::string(optarg);
      break;
    default:
      fprintf(stderr, "Unknown option\n");
      usage(argv[0]);
      exit(1);
      break;
    case 'g':
      churn_latency_bucket_size = strtol(optarg, &term, 10);
      if (*term) {
      }
      break;
    }
  }
  if (should_record) {
    printf("Recording to : %s\n", record_file.c_str());
  }

  num_slots = (1ULL << quotient_bits);
  num_initial_load_keys = ((1ULL << quotient_bits) * initial_load_factor / 100);
}

void generate_load_ops(
  std::vector<hm_op> &ops,
  std::vector<std::pair<uint64_t, uint64_t>> &kv) {
  uint64_t num_initial_load_ops = num_initial_load_keys;
  // Each Churn Op is a cycle of INSERTS and DELETES.
  uint64_t total_churn_ops = num_initial_load_ops; 
  kv.reserve(total_churn_ops);
  ops.reserve(total_churn_ops);

  // LOAD PHASE
  uint64_t *keys = new uint64_t[num_initial_load_ops];
  uint64_t *values = new uint64_t[num_initial_load_ops];
  RAND_bytes((unsigned char *)keys,
             num_initial_load_ops * sizeof(num_initial_load_ops));
  RAND_bytes((unsigned char *)values,
             num_initial_load_ops * sizeof(num_initial_load_ops));
  for (uint64_t i = 0; i < num_initial_load_ops; i++) {
    uint64_t key = (keys[i] & BITMASK(key_bits));
    uint64_t value = (values[i] & BITMASK(value_bits));
    kv.push_back(std::make_pair(key, value));
    ops.push_back(hm_op{INSERT, key, value});
  }
  delete keys;
  delete values;
}

std::vector<hm_op> generate_churn_ops(
  std::vector<hm_op> &ops,
  std::vector<std::pair<uint64_t, uint64_t>> &kv) {
  // CHURN PHASE
  uint64_t *keys_indexes_to_delete = new uint64_t[nchurn_delete_ops];
  uint64_t *keys_indexes_to_query = new uint64_t[nchurn_lookup_ops];
  uint64_t *new_keys = new uint64_t[nchurn_insert_ops];
  uint64_t *new_values = new uint64_t[nchurn_insert_ops];

  RAND_bytes((unsigned char *)new_keys, nchurn_insert_ops * sizeof(uint64_t));
  RAND_bytes((unsigned char *)new_values, nchurn_insert_ops * sizeof(uint64_t));
  RAND_bytes((unsigned char *)keys_indexes_to_delete, nchurn_delete_ops * sizeof(uint64_t));
  RAND_bytes((unsigned char *)keys_indexes_to_query, nchurn_lookup_ops * sizeof(uint64_t));

  if (mixed_workload) {
    uint64_t total_churn_ops = nchurn_delete_ops + nchurn_insert_ops + nchurn_lookup_ops;
    uint64_t num_ops[3] = {0, 0, 0};
    uint64_t total_ops[3];
    total_ops[INSERT] = nchurn_insert_ops;
    total_ops[DELETE] = nchurn_delete_ops;
    total_ops[LOOKUP] = nchurn_lookup_ops;
    uint64_t churn_op = 0;
    while (churn_op < total_churn_ops) {
      int op_choice;
      // Spin until we get a choice that has operations left.
      do {
        op_choice = rand() % 3;
      } while (num_ops[op_choice] == total_ops[op_choice]);

      if (op_choice == INSERT) {
        if (num_ops[INSERT] == num_ops[DELETE]) {
          // We don't want load factor going beyond max load factor.
          // So do a delete here instead.
          op_choice = DELETE;
        } else {
          uint64_t key = (new_keys[num_ops[INSERT]] & BITMASK(key_bits));
          uint64_t value = (new_values[num_ops[INSERT]] & BITMASK(value_bits));
          ops.push_back(hm_op{INSERT, key, value});
          // Insert the key into a slot that was just deleted.
          // Since num_deletes >= num_ops, this is guaranteed to be a deleted slot.
          uint32_t index = keys_indexes_to_delete[num_ops[INSERT]] % kv.size();
          kv[index] = make_pair(key, value);
        }
      } 
      if (op_choice == DELETE) {
        uint32_t index = keys_indexes_to_delete[num_ops[DELETE]] % kv.size();
        uint64_t key = kv[index].first;
        uint64_t value = kv[index].second;
        ops.push_back(hm_op{DELETE, key, value});
      } else if (op_choice == LOOKUP) {
        uint64_t index = keys_indexes_to_query[num_ops[LOOKUP]] % kv.size();
        uint64_t key = kv[index].first;
        uint64_t value = kv[index].second;
        ops.push_back(hm_op{LOOKUP, key, value});
      }
      num_ops[op_choice]++;
      churn_op++;
    }
  } else {
    for (int churn_op = 0; churn_op < nchurn_delete_ops; churn_op++) {
      uint32_t index = keys_indexes_to_delete[churn_op] % kv.size();
      uint64_t key = kv[index].first;
      uint64_t value = kv[index].second;
      ops.push_back(hm_op{DELETE, key, value});
    }
    for (int churn_op = 0; churn_op < nchurn_insert_ops; churn_op++) {
      uint64_t key = (new_keys[churn_op] & BITMASK(key_bits));
      uint64_t value = (new_values[churn_op] & BITMASK(value_bits));
      ops.push_back(hm_op{INSERT, key, value});
      // Insert the key into the slot that was just deleted.
      uint32_t index = keys_indexes_to_delete[churn_op] % kv.size();
      kv[index] = make_pair(key, value);
    }
    for (int churn_op = 0; churn_op < nchurn_lookup_ops; churn_op++) {
      uint64_t index = keys_indexes_to_query[churn_op] % kv.size();
      uint64_t key = kv[index].first;
      uint64_t value = kv[index].second;
      ops.push_back(hm_op{LOOKUP, key, value});
    }
  }
  delete keys_indexes_to_delete;
  delete keys_indexes_to_query;
  delete new_keys;
  delete new_values;
  return ops;
}

inline int execute_hm_op(
  vector<hm_op> &ops,
  uint64_t op_index){
  uint64_t lookup_value;
  switch (ops[op_index].op) {
    case LOOKUP:
      return g_lookup(ops[op_index].key, &lookup_value);
    case INSERT:
      return g_insert(ops[op_index].key, ops[op_index].value);
    case DELETE:
      return g_remove(ops[op_index].key);
  }
  return -1;
}


int profile_ops(
  int churn_cycle,
  std::string operation,
  vector<ThrputMeasure> &thrput_measures,
  vector<LatencyMeasure> &latency_samples,
  vector<hm_op> &ops,
  uint64_t op_start,
  uint64_t op_end,
  uint64_t throughput_bucket_size,
  bool should_measure_latency) {

  uint64_t ops_executed = 0;
  time_point<high_resolution_clock> throughput_measure_begin, throughput_measure_end;
  time_point<high_resolution_clock> latency_measure_begin, latency_measure_end;
  for (uint64_t i=op_start; i<op_end; i++) {
    if (ops_executed % throughput_bucket_size == 0) {
      if (ops_executed) {
        throughput_measure_end = high_resolution_clock::now();
        thrput_measures.push_back({
            churn_cycle,
            operation,
            throughput_measure_begin, 
            throughput_measure_end,
            throughput_bucket_size
          });
      }
      throughput_measure_begin = high_resolution_clock::now();
    }
    if (should_measure_latency && ops_executed % churn_latency_bucket_size == 0) {
      if (ops_executed) {
        latency_measure_end = high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(latency_measure_end - latency_measure_begin);
        latency_samples.push_back({
          operation,
          (uint64_t)duration.count()
        });
      }
      latency_measure_begin = high_resolution_clock::now();
    }
    int status = execute_hm_op(ops, i);
    if (ops[i].op == INSERT && status == QF_NO_SPACE) {
      // DIED. Handle Death.
      return -1;
    }
    ops_executed++;
  }
  if (ops_executed) {
    throughput_measure_end = high_resolution_clock::now();
    thrput_measures.push_back({
      churn_cycle,
      operation,
      throughput_measure_begin, 
      throughput_measure_end,
      ops_executed % throughput_bucket_size == 0 ? throughput_bucket_size : ops_executed % throughput_bucket_size
    });
  }
  return 0;
}

void run_churn(
				std::vector<hm_op> &ops, 
        std::vector<std::pair<uint64_t, uint64_t>> &kv,
        uint64_t start, 
        std::string thrput_output_file,
        std::string latency_output_file,
        std::string metadata_output_file) {
  vector<ThrputMeasure> thrput_measures;
  vector<LatencyMeasure> latency_measures;
  vector<HmMetadataMeasure> metadata_measures;
  uint64_t throughput_ops_per_bucket; 
  int status = 0;
  int churn_start_op = start;
  bool should_measure_latency = false;

  auto test_begin = chrono::high_resolution_clock::now();
  //Write headers
  write_churn_thrput_by_phase_to_file(thrput_measures, test_begin, true, thrput_output_file);
  write_churn_latency_by_phase_to_file(latency_measures, true, latency_output_file);
  write_churn_metadata_to_file(metadata_measures, test_begin, true, metadata_output_file);

  for (int i=0; i<nchurns; i++) {
    ops.clear();
    churn_start_op = 0;
    generate_churn_ops(ops, kv);
    fprintf(LOG, "Running Churn cycle: %d\n", i);
    should_measure_latency = (nchurns - i < churn_window_for_latency);
    if (mixed_workload) {
      int nchurn_ops = nchurn_insert_ops + nchurn_delete_ops + nchurn_lookup_ops;
      throughput_ops_per_bucket = nchurn_ops / churn_thrput_resolution;
      status = profile_ops(i, "MIXED", thrput_measures, latency_measures, ops, churn_start_op, churn_start_op + nchurn_ops, throughput_ops_per_bucket, should_measure_latency);
      if (status) break;
      churn_start_op += nchurn_ops;
    } else {
      // DELETE
      throughput_ops_per_bucket = nchurn_delete_ops / churn_thrput_resolution;
      status = profile_ops(i, "DELETE", thrput_measures, latency_measures, ops, churn_start_op, churn_start_op+ nchurn_delete_ops, throughput_ops_per_bucket, should_measure_latency);
      if (status) break;
      churn_start_op += nchurn_delete_ops;
      // INSERT 
      throughput_ops_per_bucket = nchurn_insert_ops / churn_thrput_resolution;
      status = profile_ops(i, "INSERT", thrput_measures, latency_measures, ops, churn_start_op, churn_start_op+ nchurn_insert_ops, throughput_ops_per_bucket, should_measure_latency);
      if (status) break;
      churn_start_op += nchurn_insert_ops;
      // LOOKUP
      throughput_ops_per_bucket = nchurn_lookup_ops / churn_thrput_resolution;
      status = profile_ops(i, "LOOKUP", thrput_measures, latency_measures, ops, churn_start_op, churn_start_op+ nchurn_lookup_ops, throughput_ops_per_bucket, should_measure_latency);
      if (status) break;
      churn_start_op += nchurn_lookup_ops;
    }

  #ifdef USE_ABSL
    metadata_measures.push_back({
      i,
      high_resolution_clock::now(),
      g_map.size(),
      g_map.bucket_count() - g_map.size()
    });
  #endif

  #ifdef USE_ICEBERG
    metadata_measures.push_back({
      i,
      high_resolution_clock::now(),
      0, // NA
      0 // NA
    });
  #endif

  #ifdef USE_CLHT
    metadata_measures.push_back({
      i,
      high_resolution_clock::now(),
      0, // NA
      0 // NA
    });
  #endif

  #ifndef USE_ABSL
  #ifndef USE_ICEBERG
  #ifndef USE_CLHT
    metadata_measures.push_back({
      i,
      high_resolution_clock::now(),
      g_hashmap.metadata->nelts,
#ifdef QF_TOMBSTONE
      g_hashmap.metadata->noccupied_slots - g_hashmap.metadata->nelts
#else
      0
#endif
    });
    #endif
    #endif
    #endif

    // Flush logs
    if (i % log_commit_freq == 0) {
      write_churn_thrput_by_phase_to_file(thrput_measures, test_begin, false, thrput_output_file);
      write_churn_latency_by_phase_to_file(latency_measures, false, latency_output_file);
      write_churn_metadata_to_file(metadata_measures, test_begin, false, metadata_output_file);
    }
  }
  write_churn_thrput_by_phase_to_file(thrput_measures, test_begin, false, thrput_output_file);
  write_churn_latency_by_phase_to_file(latency_measures, false, latency_output_file);
  write_churn_metadata_to_file(metadata_measures, test_begin, false, metadata_output_file);
}

void run_load(std::vector<hm_op> &ops, uint64_t num_initial_load_keys, size_t npoints, std::string output_file) {
  time_point<high_resolution_clock> ts[2 * npoints];
  time_point<high_resolution_clock> load_begin_ts;
  time_point<high_resolution_clock> load_end_ts;
  uint64_t nops = num_initial_load_keys;
  uint64_t i, j;
  load_begin_ts = high_resolution_clock::now();
  for (size_t exp = 0; exp < 2 * npoints; exp += 2) {
    i = (exp / 2) * (nops / npoints);
    j = ((exp / 2) + 1) * (nops / npoints);
    fprintf(LOG, "Round: %lu OPS %s [%lu %lu]\n", exp, output_file.c_str(), i, j);

    ts[exp] = high_resolution_clock::now();
    int ret = 0;
    for (uint64_t op_idx = i; op_idx < j; op_idx++) {
      hm_op op = ops[op_idx];
  #if DEBUG
      assert(op.op == INSERT);
  #endif
      ret = g_insert(op.key, op.value);
      if (ret == QF_NO_SPACE)
        break;
    }
    if (ret == QF_NO_SPACE)
      ts[exp+1] = ts[exp];
    else 
      ts[exp+1] = high_resolution_clock::now();
  }
  load_end_ts = high_resolution_clock::now();
  auto load_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(load_end_ts - load_begin_ts).count();
  write_load_thrput_to_file(ts, npoints, output_file, nops);
  printf("overall load insert throughput (ops/microsec): %f\n", num_initial_load_keys/(load_duration * 0.001));
}

void setup(std::string dir) {
  std::string mkdir = "mkdir -p " + dir;
  assert(system(mkdir.c_str()) == 0);
}

void write_test_params() {
  std::string test_params = "test_params.txt";
  ofstream ofs;
  ofs.open(dir + test_params);
  ofs<< g_memory_usage() << " " << endl;
  ofs<< key_bits << " " <<endl;
  ofs<< quotient_bits << " " <<endl;
  ofs<< value_bits << " " <<endl;
  ofs<< initial_load_factor << " " <<endl;
  ofs<< nchurns << " " << endl;
  ofs<< nchurn_insert_ops << " " << endl;
  ofs<< nchurn_delete_ops << " " << endl;
  ofs<< nchurn_lookup_ops << " " << endl;
  ofs.close();
}

void churn_test() {
  std::string load_op = "load.txt";
  std::string churn_thrput = "churn_thrput.txt";
  std::string churn_latency = "churn_latency.txt";
  std::string churn_metadata  = "churn_metadata.txt";
  std::string filename_load = dir + load_op;
  std::string filename_churn_thrput = dir +  churn_thrput;
  std::string filename_churn_latency = dir +  churn_latency;
  std::string filename_churn_metadata  = dir +  churn_metadata;
  float max_load_factor = initial_load_factor / 100.0;
  printf("max_load_factor: %f\n", max_load_factor);

  std::vector<hm_op> ops;
  std::vector<std::pair<uint64_t, uint64_t>> kv;
  generate_load_ops(ops, kv);

  g_init(num_slots, key_bits, value_bits, max_load_factor);
  write_test_params();
  // LOAD PHASE.
  run_load(ops, num_initial_load_keys, npoints, filename_load);
  // CHURN PHASE.
  run_churn(ops, kv, num_initial_load_keys, filename_churn_thrput, filename_churn_latency, filename_churn_metadata);
  g_dump_metrics(dir);
  g_destroy();
}


int main(int argc, char **argv) {
  parseArgs(argc, argv);
  if (is_silent) {
    LOG = fopen("/dev/null", "w");
  } else {
    LOG = stderr;
  }
  setup(dir);

	churn_test();

  return 0;
}
