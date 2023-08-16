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

#define MAX_VALUE(nbits) ((1ULL << (nbits)) - 1)
#define BITMASK(nbits) ((nbits) == 64 ? 0xffffffffffffffff : MAX_VALUE(nbits))

int key_bits = 16;
int quotient_bits = 8;
int value_bits = 8;
int initial_load_factor = 95;
int nchurns = 10;
int nchurn_ops = 500;
int npoints = 50;
int should_record = 0;
std::string datastruct = "all";
std::string record_file = "test_case.txt";
std::string dir = "./bench_run/";
hashmap hashmap_ds = rhm;
uint64_t num_slots = 0;
uint64_t num_initial_load_keys = 0;
bool is_silent = true;
FILE *LOG;

void write_thrput_to_file(time_point<high_resolution_clock> *ts, uint64_t npoints,
                          std::string filename, uint64_t num_ops) {
  FILE *fp = fopen(filename.c_str(), "w");
  fprintf(fp, "x_0    y_0\n");
  for (uint64_t exp = 0; exp < 2 * npoints; exp += 2) {
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(ts[exp+1] - ts[exp]);
    fprintf(fp, "%f", ((exp / 2.0) * (100.0 / npoints)));
    fprintf(fp, " %f",
            (num_ops / npoints) / duration.count());
    fprintf(fp, "\n");
  }
  fclose(fp);
}

void usage(char *name) {
  printf(
      "%s [OPTIONS]\n"
      "Options are:\n"
      "  -k keybits            [ Size of key in bits. ]\n"
      "  -q quotient_bits      [ Size of quotient in bits. ]\n"
      "  -v value_keybits      [ Size of value in bits. ]\n"
      "  -i initial load       [ Initial Load Factor[0-100]. Default 94 ]\n"
      "  -c churn cycles       [ Number of churn cycles.  Default 10 ]\n"
      "  -l churn length       [ Number of insert, delete operations per churn "
      "cycle ]\n"
      "  -d datastruct         [ Default all. rhm, trhm, gzhm, grhm. ]\n"
      "  -r record             [ Whether to record. If 1 will record to -f. Use test_runner to replay or check test case.]\n"
      "  -f record/replay file [ File to record to. Default test_case.txt ]"
      "  -p npoints            [ number of points on the graph.  Default 20 "
      "  -s silent             [ Default 1. Use 0 for verbose mode"
      "]\n",
      name);
}

void parseArgs(int argc, char **argv) {
  /* Argument parsing */
  int opt;
  char *term;

  while ((opt = getopt(argc, argv, "k:q:v:i:c:l:d:f:p:r:s:")) != -1) {
    switch (opt) {
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
      nchurn_ops = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -l must be an integer\n");
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
    case 'd':
      datastruct = std::string(optarg);
      break;
    case 'f':
      record_file = std::string(optarg);
      break;
    default:
      fprintf(stderr, "Unknown option\n");
      usage(argv[0]);
      exit(1);
      break;
    }
  }
  if (should_record) {
    printf("Recording to : %s\n", record_file.c_str());
  }

  num_slots = (1ULL << quotient_bits);
  num_initial_load_keys = ((1ULL << quotient_bits) * initial_load_factor / 100);
}


std::vector<hm_op> generate_ops() {
  uint64_t num_initial_load_ops = num_initial_load_keys;
  // Each Churn Op is a cycle of INSERTS and DELETES.
  uint64_t total_churn_ops = num_initial_load_ops + (nchurns * nchurn_ops * 2);

  std::vector<std::pair<uint64_t, uint64_t>> kv;
  std::vector<hm_op> ops;
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

  // CHURN PHASE
  uint64_t *keys_indexes_to_delete = new uint64_t[nchurn_ops];
  uint64_t *new_keys = new uint64_t[nchurn_ops];
  uint64_t *new_values = new uint64_t[nchurn_ops];
  for (int churn_cycle = 0; churn_cycle < nchurns; churn_cycle++) {
    fprintf(LOG, "Generating for churn %d\n", churn_cycle);
    RAND_bytes((unsigned char *)new_keys, nchurn_ops * sizeof(uint64_t));
    RAND_bytes((unsigned char *)new_values, nchurn_ops * sizeof(uint64_t));
    RAND_bytes((unsigned char *)keys_indexes_to_delete, nchurn_ops * sizeof(uint64_t));

    for (uint64_t churn_op = 0; churn_op < nchurn_ops; churn_op++) {
      uint32_t index = keys_indexes_to_delete[churn_op] % kv.size();
      uint64_t key = kv[index].first;
      uint64_t value = kv[index].second;
      ops.push_back(hm_op{DELETE, key, value});
    }
    for (uint64_t churn_op = 0; churn_op < nchurn_ops; churn_op++) {
      uint64_t key = (new_keys[churn_op] & BITMASK(key_bits));
      uint64_t value = (new_values[churn_op] & BITMASK(value_bits));
      ops.push_back(hm_op{INSERT, key, value});
      // Insert the key into the slot that was just deleted.
      uint32_t index = keys_indexes_to_delete[churn_op] % kv.size();
      kv[index] = make_pair(key, value);
    }
  }
  return ops;
}

void run_ops(std::string datastruct_name, std::string phase_name,
    std::vector<hm_op> &ops, uint64_t start, uint64_t end, int npoints,
             std::string output_file) {
  printf("Beginning %s\n", phase_name.c_str());
  time_point<high_resolution_clock> ts[2 * npoints];
  double op_durtn[3];
  uint64_t op_cnt[3];

  op_durtn[INSERT] = 0;
  op_durtn[DELETE] = 0;
  op_durtn[LOOKUP] = 0;
  op_cnt[INSERT] = 0;
  op_cnt[DELETE] = 0;
  op_cnt[LOOKUP] = 0;
  time_point<high_resolution_clock> op_begin_ts;
  time_point<high_resolution_clock> op_end_ts;
  int cur_op = 0;
  uint64_t num_op = 0;

  uint64_t nops = (end - start);
  uint64_t i, j, lookup_value = 0;
  for (uint64_t exp = 0; exp < 2 * npoints; exp += 2) {
    i = (exp / 2) * (nops / npoints) + start;
    j = ((exp / 2) + 1) * (nops / npoints) + start;
    fprintf(LOG, "Round: %d OPS %s [%lu %lu]\n", exp, output_file.c_str(), i, j);

    // TODO: Record time for this batch.
    ts[exp] = high_resolution_clock::now();
    for (uint64_t op_idx = i; op_idx < j; op_idx++) {
      if (op_idx == 0 || ops[op_idx].op != cur_op) {
        if (num_op) {
          op_end_ts = high_resolution_clock::now();
          op_durtn[cur_op] += duration_cast<nanoseconds>(op_end_ts - op_begin_ts).count();
          op_cnt[cur_op] += num_op;
        }
        op_begin_ts = high_resolution_clock::now();
        num_op = 0;
      }
      cur_op = ops[op_idx].op;
      hm_op op = ops[op_idx];
      num_op++;
      switch (op.op) {
      case INSERT:
        hashmap_ds.insert(op.key, op.value);
        break;
      case DELETE:
        hashmap_ds.remove(op.key);
        break;
      case LOOKUP:
        hashmap_ds.lookup(op.key, &lookup_value);
        break;
      }
    }
    ts[exp+1] = high_resolution_clock::now();
  }
  if (num_op) {
    op_end_ts = high_resolution_clock::now();
    op_durtn[cur_op] += duration_cast<nanoseconds>(op_end_ts - op_begin_ts).count();
    op_cnt[cur_op] += num_op;
  }
  write_thrput_to_file(ts, npoints, output_file, nops);
  printf("%s insert throughput (ops/microsec): %f\n", datastruct_name.c_str(), 0.001 * op_durtn[INSERT] / op_cnt[INSERT]);
  printf("%s delete throughput (ops/microsec): %f\n", datastruct_name.c_str(), 0.001 * op_durtn[DELETE] / op_cnt[DELETE]);
  printf("%s lookup throughput (ops/microsec): %f\n", datastruct_name.c_str(), 0.001 * op_durtn[LOOKUP] / op_cnt[LOOKUP]);
}

void setup(std::string dir) {
  std::string mkdir = "mkdir -p " + dir;
  system(mkdir.c_str());
}

void run_churn(std::string datastruct_name, hashmap hashmap_ds, std::vector<hm_op> &ops) {
  std::string load_op = "load.txt";
  std::string churn_op = "churn.txt";
  std::string filename_load = dir + datastruct_name + "-" + load_op;
  std::string filename_churn = dir + datastruct_name + "-" + churn_op;
  hashmap_ds.init(num_slots, key_bits, value_bits);
  // LOAD PHASE.
  run_ops(datastruct_name, "load phase", ops, 0, num_initial_load_keys, npoints, filename_load);
  // CHURN PHASE.
  run_ops(datastruct_name, "churn phase", ops, num_initial_load_keys, ops.size(), npoints, filename_churn);
  hashmap_ds.destroy();
}

void write_test_params(std::vector<hm_op> ops) {
  std::string test_params = "test_params.txt";
  ofstream ofs;
  ofs.open(dir + test_params);
  ofs<< key_bits << " " <<endl;
  ofs<< quotient_bits << " " <<endl;
  ofs<< value_bits << " " <<endl;
  ofs<< initial_load_factor << " " <<endl;
  ofs<< nchurns << " " << endl;
  ofs<< nchurn_ops << " " << endl;

  for (uint64_t i=num_initial_load_keys+1; i<ops.size(); i++) {
    if (ops[i].op != ops[i-1].op) {
      ofs << (i - num_initial_load_keys) / (1.0 * ((ops.size() - num_initial_load_keys))) * 100.0 << endl;
    }
  }
  ofs.close();
}

int main(int argc, char **argv) {
  parseArgs(argc, argv);
  if (is_silent) {
    LOG = fopen("/dev/null", "w");
  } else {
    LOG = stderr;
  }
  std::string outputfile = "thrput";
  std::string replay_op = "replay.txt\0";

  setup(dir);

  printf("DS: %s\n", datastruct.c_str());

  std::vector<hm_op> ops;
  ops = generate_ops();
  write_test_params(ops);
  if (should_record) {
    write_ops(record_file, key_bits, quotient_bits, value_bits, ops);
  }

  if (datastruct == "all") {
    run_churn("rhm", rhm, ops);
    run_churn("trhm", trhm, ops);
    run_churn("grhm", grhm, ops);
    run_churn("gzhm", gzhm, ops);
  } else {
  if (datastruct == "rhm") {
    hashmap_ds = rhm;
  } else if (datastruct == "trhm") {
    hashmap_ds = trhm;
  } else if (datastruct == "grhm") {
    hashmap_ds = grhm;
  } else if (datastruct == "gzhm") {
    hashmap_ds = gzhm;
  } else {
    fprintf(stderr, "Unknown datastruct.\n");
    usage(argv[0]);
    exit(1);
  }
  run_churn(datastruct, hashmap_ds, ops);
  }
  return 0;
}