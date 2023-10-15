#include <fstream>
#include <iostream>
#include <map>
#include <openssl/rand.h>
#include <set>
#include <unistd.h>
#include <vector>
#include <cassert>
#include "hm_op.h"
#include "hm_wrapper.h"

using namespace std;

#define MAX_VALUE(nbits) ((1ULL << (nbits)) - 1)
#define BITMASK(nbits) ((nbits) == 64 ? 0xffffffffffffffff : MAX_VALUE(nbits))


uint64_t get_random_key(std::map<uint64_t, uint64_t> map, int key_bits) {
  uint64_t rand_idx = 0;
  rand_idx = rand_idx % map.size();
  // This is not the best way to do this.
  auto iter = map.begin();
  std::advance(iter, rand_idx);
  return iter->first;
}

void generate_ops(int key_bits, int quotient_bits, int value_bits,
                  int initial_load_factor, int num_ops,
                  std::vector<hm_op> &ops) {
  uint64_t nkeys = ((1ULL << quotient_bits) * initial_load_factor) / 100;
  uint64_t *keys = new uint64_t[nkeys];
  uint64_t *values = new uint64_t[nkeys];

  RAND_bytes((unsigned char *)keys, nkeys * sizeof(uint64_t));
  RAND_bytes((unsigned char *)values, nkeys * sizeof(uint64_t));

  std::map<uint64_t, uint64_t> map;
  std::vector<uint64_t> deleted_keys;

  for (size_t i = 0; i < nkeys; i++) {
    uint64_t key = keys[i] & BITMASK(key_bits);
    uint64_t value = values[i] & BITMASK(value_bits);
    if (value == 0) value = 1; // CLHT returns 0 on not found, so that confuses the test.
    ops.push_back({INSERT, key, value});
    map[key] = value;
  }

  delete keys;
  delete values;

  values = new uint64_t[num_ops];
  keys = new uint64_t[num_ops];
  RAND_bytes((unsigned char *)values, num_ops * sizeof(uint64_t));
  RAND_bytes((unsigned char *)keys, num_ops * sizeof(uint64_t));

  for (int i = 0; i < num_ops; i++) {
    int op_type = rand() % 3;
    int existing = rand() % 2;
    int deleted = rand() % 2;
    uint64_t key;
    uint64_t new_value;
    uint64_t existing_value;

    if (existing && map.size() > 0) {
      key = get_random_key(map, key_bits);
    } else if (deleted && deleted_keys.size() > 0) {
      key = deleted_keys[rand() % deleted_keys.size()];
    } else {
        key = keys[i] & BITMASK(key_bits);
    }

    if (map.find(key) != map.end()) {
        existing_value = map[key];
    } else {
        existing_value = -1;
    }
    new_value = values[i] & BITMASK(value_bits);
    if (new_value == 0) new_value = 1;

    switch (op_type) {
    case INSERT:
      if (map.size() > nkeys)
        break;
      map[key] = new_value;
      break;
    case DELETE:
      ops.push_back({DELETE, key, existing_value});
      if (existing_value != -1u) {
        deleted_keys.push_back(key);
        map.erase(key);
      }
    case LOOKUP:
      ops.push_back({LOOKUP, key, existing_value});
      break;
    default:
      break;
    }
  }
  delete keys;
  delete values;
}

int key_bits = 16;
int quotient_bits = 8;
int value_bits = 8;
int initial_load_factor = 50;
int num_ops = 200;
bool should_replay = false;
std::string replay_file = "test_case.txt";
std::map<uint64_t, uint64_t> current_state;
static int verbose_flag = 0;  // 1 for verbose, 0 for brief

void check_universe(uint64_t key_bits, std::map<uint64_t, uint64_t> expected, bool check_equality = false) {
  uint64_t value;
  for (uint64_t k = 0; k <= (1UL<<key_bits)-1; k++) {
    int key_exists = expected.find(k) != expected.end();
    int ret = g_lookup(k, &value);
    if (key_exists) {
      uint64_t expected_value = expected[k];
      if (ret < 0 && expected_value != value) {
        fprintf(stderr, "Key %lx, %lu value expected: %lu actual: %lu\n", k, k, expected_value, value);
        abort();
      }
      if (check_equality) assert(expected_value == value);
    } else {
      if (ret != QF_DOESNT_EXIST) {
        fprintf(stderr, "Key %lx, %lu should not exist.\n", k, k);
        abort();
      }
    }
  }
}

void usage(char *name) {
  printf("%s [OPTIONS]\n"
         "Options are:\n"
         "  -k keysize bits         [ log_2 of map capacity.  Default 16 ]\n"
         "  -q quotientbits         [ Default 8. Max 64.]\n"
         "  -v value bits           [ Default 8. Max 64.]\n"
         "  -m initial load factor  [ Initial Load Factor[0-100]. Default 50. ]\n"
         "  -l                      [ Random Ops. Default 50.]\n"
         "  -r replay               [ Whether to replay. If 0, will record to -f ]\n"
         "  -f file                 [ File to record. Default test_case.txt ]\n",
         name);
}

void parseArgs(int argc, char **argv) {
  int opt;
  char *term;

  while ((opt = getopt(argc, argv, "k:q:v:m:l:f:r:")) != -1) {
    switch (opt) {
    case 'k':
      key_bits = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -n must be an integer\n");
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
    case 'm':
      initial_load_factor = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -m must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'l':
      num_ops = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -l must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'r':
        should_replay = strtol(optarg, &term, 10);
        if (*term) {
            fprintf(stderr, "Argument to -r must be an integer (0 to disable) \n");
            usage(argv[0]);
            exit(1);
        }
        break;
    case 'f':
        replay_file = std::string(optarg);
        break;
    }
    // TODO(chesetti): Add assertions that flags are sane.
  }
}

int main(int argc, char **argv) {
  parseArgs(argc, argv);
  cout << "Key Bits: " << key_bits << std::endl;
  cout << "Quotient Bits: " << quotient_bits << std::endl;
  cout << "Value Bits: " << value_bits << std::endl;
  float max_load_factor = initial_load_factor / 100.0;
  cout << "LoadFactor : " << max_load_factor << std::endl;
  cout << "Num Ops: " << num_ops << std::endl;
  cout << "Is Replay: " << should_replay << std::endl;
  cout << "Test Case Replay File: " << replay_file << std::endl;

  std::vector<hm_op> ops;
  if (should_replay) {
    load_ops(replay_file, &key_bits, &quotient_bits, &value_bits, ops);
  } else {
    generate_ops(key_bits, quotient_bits, value_bits, initial_load_factor,
               num_ops, ops);
  }
  write_ops(replay_file, key_bits, quotient_bits, value_bits, ops);

  std::map<uint64_t, uint64_t> map;
  g_init((1ULL<<quotient_bits), key_bits, value_bits, max_load_factor);
  uint64_t key, value;
  int ret, key_exists;
  for (size_t i=0; i < ops.size(); i++) {
    auto op = ops[i];
    key = op.key;
    value = op.value;
    if (verbose_flag)
      printf("%lu op: %d, key: %lx, value:%lx.\n", i, op.op, key, value);
    switch(op.op) {
      case INSERT:
        map[key] = value;
        ret = g_insert(key, value);
        if (ret < 0 && ret != QF_KEY_EXISTS) {
          fprintf(stderr, "Insert failed. Return %d for key %lx.\n", ret, key);
          fprintf(stderr, "Replay this testcase with ./test_case -r 1 -f %s\n", replay_file.c_str());
          abort();
        }
        check_universe(key_bits, map);
        break;
      case DELETE:
        key_exists = map.erase(key);
        if (verbose_flag)
          printf("key_exists: %d\n", key_exists);
        ret = g_remove(key);
        if (key_exists && ret < 0) {
          fprintf(stderr, "Delete failed. Return %d for existing key %lx.\n", ret, key);
          fprintf(stderr, "Replay this testcase with ./test_case -r 1 -f %s\n", replay_file.c_str());
          abort();
        }
        check_universe(key_bits, map);
        break;
      case LOOKUP:
        ret = g_lookup(key, &value);
        if (map.find(key) != map.end()) {
          if (ret < 0)  {
            fprintf(stderr, "Find failed. Return %d for existing key %lx.\n", ret, key);
						fprintf(stderr, "Replay this testcase with ./test_case -r 1 -f %s\n", replay_file.c_str());
            abort();
          }
        }
        break;
    }
  }
  printf("Test success.\n");
  g_destroy();
}
