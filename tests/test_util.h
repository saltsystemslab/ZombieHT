#ifndef TEST_UTIL_H
#define TEST_UTIL_H

#include <iostream>
#include <fstream>
#include "rhm_wrapper.h"
#include "trhm_wrapper.h"
#include "trhm_norebuild_wrapper.h"
#include "grhm_wrapper.h"
#include <cassert>
using namespace std;

#define MAX_VALUE(nbits) ((1ULL << (nbits)) - 1)
#define BITMASK(nbits) ((nbits) == 64 ? 0xffffffffffffffff : MAX_VALUE(nbits))


typedef int (*init_op)(uint64_t nkeys, uint64_t key_bits, uint64_t value_bits);
typedef int (*insert_op)(uint64_t key, uint64_t val);
typedef int (*lookup_op)(uint64_t key, uint64_t *val);
typedef int (*remove_op)(uint64_t key);
typedef int (*destroy_op)();

typedef struct hashmap {
  init_op init;
  insert_op insert;
  lookup_op lookup;
  remove_op remove;
  destroy_op destroy;
} hashmap;

hashmap rhm = {g_rhm_init, g_rhm_insert, g_rhm_lookup, g_rhm_remove,
               g_rhm_destroy};
hashmap trhm = {g_trhm_init, g_trhm_insert, g_trhm_lookup, g_trhm_remove,
               g_trhm_destroy};
hashmap trhm_nr = {g_trhm_nr_init, g_trhm_nr_insert, g_trhm_nr_lookup, g_trhm_nr_remove,
               g_trhm_nr_destroy};
#if 0
hashmap grhm = {g_grhm_init, g_grhm_insert, g_grhm_lookup, g_grhm_remove,
               g_grhm_rebuild, g_grhm_destroy};
#endif

#define INSERT 0
#define DELETE 1
#define LOOKUP 2

struct hm_op {
  int op;
  uint64_t key;
  uint64_t value;
};

void load_ops(std::string replay_filepath, int *key_bits, int *quotient_bits,
              int *value_bits, std::vector<hm_op> &ops) {
  ifstream ifs;
  ifs.open(replay_filepath);
  ifs >> (*key_bits) >> (*quotient_bits) >> (*value_bits);
  std::cout << (*key_bits) << " " << (*quotient_bits) << " "<< (*value_bits) << std::endl;
  uint64_t num_keys;
  ifs >> num_keys;
  for (uint64_t i = 0; i < num_keys; i++) {
    hm_op op;
    ifs >> op.op >> op.key >> op.value;
    assert(op.key < (1UL<<*key_bits));
    ops.push_back(op);
  }
  ifs.close();
}

void write_ops(std::string replay_filepath, int key_bits, int quotient_bits,
               int value_bits, std::vector<hm_op> &ops) {
  ofstream ofs;
  ofs.open(replay_filepath);
  ofs << key_bits << " " << quotient_bits << " " << value_bits << std::endl;
  ofs << ops.size() << std::endl;
  for (auto op : ops) {
    ofs << op.op << " " << op.key << " " << op.value << "\n";
  }
  ofs << std::endl;
  ofs.close();
}

#endif