#include <iostream>
#include <openssl/rand.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <iostream>
#include <chrono>

using namespace std;
using namespace std::chrono;

#include "gqf.h"
#include "hm.h"
#include "iceberg_table.h"

float load_factor = 0.95;
uint64_t ncommon_keys = 20000;
uint64_t key_size = 64;
uint64_t log_slots = 27;
uint64_t nslots = (1ULL << log_slots);

HM zomb1;
HM zomb2;
HM zomb_result;
iceberg_table ice1;
iceberg_table ice2;
iceberg_table ice3_result;

void join_test() {
  // Let's fill up with 0.95 * 2^27 Rand Keys.
  uint64_t num_keys = load_factor * nslots - ncommon_keys; 
  printf("nslots: %ld num_keys: %ld ncommon_keys: %ld\n", nslots, num_keys, ncommon_keys);

  uint64_t *key_set_1 = new uint64_t[num_keys];
  uint64_t *key_set_2 = new uint64_t[num_keys];
  uint64_t *common_keys = new uint64_t[ncommon_keys];
  uint64_t bytes_to_alloc = num_keys * sizeof(uint64_t);

  RAND_bytes((unsigned char *)key_set_1, bytes_to_alloc);
  RAND_bytes((unsigned char *)key_set_2, bytes_to_alloc);
  RAND_bytes((unsigned char *)common_keys, ncommon_keys * sizeof(uint64_t));

  // Initialize GZHM, ICEBERG
  iceberg_init(&ice1, log_slots);
  iceberg_init(&ice2, log_slots);
  iceberg_init(&ice3_result, log_slots);
  hm_malloc(&zomb1, nslots, key_size, 0 /* value_size */, QF_HASH_NONE, 0, load_factor);
  hm_malloc(&zomb2, nslots, key_size, 0 /* value_size */, QF_HASH_NONE, 0, load_factor);
  hm_malloc(&zomb_result, nslots, key_size, 0 /* value_size */, QF_HASH_NONE, 0, load_factor);

  // Fill up GZHM and ICEBERG
  for (uint64_t i=0; i < num_keys; i++) {
    hm_insert(&zomb1, key_set_1[i], 0, QF_NO_LOCK | QF_KEY_IS_HASH);
    hm_insert(&zomb2, key_set_2[i], 0, QF_NO_LOCK | QF_KEY_IS_HASH);
    iceberg_insert(&ice1, key_set_1[i], 0, 0);
    iceberg_insert(&ice2, key_set_2[i], 0, 0);
  }

  for (uint64_t i=0; i < ncommon_keys; i++) {
    hm_insert(&zomb1, common_keys[i], 0, QF_NO_LOCK | QF_KEY_IS_HASH);
    hm_insert(&zomb2, common_keys[i], 0, QF_NO_LOCK | QF_KEY_IS_HASH);
    iceberg_insert(&ice1, common_keys[i], 0, 0);
    iceberg_insert(&ice2, common_keys[i], 0, 0);
  }

	// TODO: Time this
  time_point<high_resolution_clock> qf_join_begin, qf_join_end;
  qf_join_begin = high_resolution_clock::now();
	qf_join(&zomb1, &zomb2, &zomb_result);
  qf_join_end = high_resolution_clock::now();
  auto qf_join_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(qf_join_end - qf_join_begin);
  std::cout<<qf_join_duration.count()<<std::endl;

  time_point<high_resolution_clock> ice_join_begin, ice_join_end;
  ice_join_begin = high_resolution_clock::now();
  uint64_t value;
  uint64_t count = 0;
  for (uint64_t i=0; i < num_keys; i++) {
    if (iceberg_get_value(&ice2, key_set_1[i], &value, 0)) {
      iceberg_insert(&ice3_result, key_set_1[i], value, 0);
      count++;
    }
  }
  for (uint64_t i=0; i < ncommon_keys; i++) {
    if (iceberg_get_value(&ice2, common_keys[i], &value, 0)) {
      iceberg_insert(&ice3_result, common_keys[i], value, 0);
      count++;
    }
  }
  ice_join_end = high_resolution_clock::now();
  auto ice_join_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(ice_join_end - ice_join_begin);
  std::cout<<ice_join_duration.count()<<std::endl;
    printf("Ice CommonKeys: %ld\n", count);

  delete key_set_1;
  delete key_set_2;
  delete common_keys;
  // Delete iceberg?
  // Delete gzhm.

}


int main(int argc, char **argv) {
  join_test();
  return 0;
}
