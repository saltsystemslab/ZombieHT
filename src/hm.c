#include "gqf.h"
#include "hm.h"
#include "qf.h"
#include <stdlib.h>

uint64_t hm_init(HM *hm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len) {
  abort();
}


bool hm_malloc(HM *hm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed) {
  return qf_malloc(hm, nslots, key_bits, value_bits, hash, seed);
}

void hm_destroy(HM *hm) {
  qf_destroy(hm);
}

bool hm_free(HM *hm) {
  return qf_free(hm);
}

int hm_insert(HM *hm, uint64_t key, uint64_t value, uint8_t flags) {
  return qf_insert(hm, key, value, flags);
}

int hm_remove(HM *hm, uint64_t key, uint8_t flags) {
  return qf_remove(hm, key, flags);
}

int hm_lookup(const QF *hm, uint64_t key, uint64_t *value, uint8_t flags) {
  return qf_lookup(hm, key, value, flags);
}
