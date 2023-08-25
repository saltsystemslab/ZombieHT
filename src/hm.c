#include "gqf.h"
#include "hm.h"

#ifdef QF_TOMBSTONE
#include "qft.h"
#else
#include "qf.h"
#endif

#include <stdlib.h>
#include <stdio.h>

uint64_t hm_init(HM *hm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len) {
  abort();
}


bool hm_malloc(HM *hm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed) {

  int ret = qf_malloc(hm, nslots, key_bits, value_bits, hash, seed);
#ifdef QF_TOMBSTONE
  reset_rebuild_cd(hm);
#endif
  return ret;
}

void hm_destroy(HM *hm) {
  qf_destroy(hm);
}

bool hm_free(HM *hm) {
  return qf_free(hm);
}

int hm_rebuild(HM *hm, uint8_t flags) {
#ifdef QF_TOMBSTONE
    qft_rebuild(hm, flags);
    return 0;
#else
    return 0;
#endif
}

int hm_insert(HM *hm, uint64_t key, uint64_t value, uint8_t flags) {

#ifdef QF_TOMBSTONE
  int ret = qft_insert(hm, key, value, flags);
  if (ret == QF_KEY_EXISTS) return ret;

#ifdef REBUILD_DEAMORTIZED_GRAVEYARD
  if (ret < 0)
    abort();
  _deamortized_rebuild(hm);
#elif REBUILD_AT_INSERT
  if (ret < 0)
    return ret;
  _deamortized_rebuild(hm, key, flags);
#else 
  if (ret == QF_NO_SPACE) {
    hm_rebuild(hm, flags);
    ret = qft_insert(hm, key, value, flags);
  }
  if (ret == QF_KEY_EXISTS) return ret;
  if (ret < 0) {
    // fprintf(stderr, "Insert failed: %d\n", ret);
    return ret;
  }
  if (--(hm->metadata->rebuild_cd) == 0) {
    int ret_rebuild = hm_rebuild(hm, flags);
    if (ret_rebuild < 0) {
      if (ret_rebuild == QF_NO_SPACE) {
        fprintf(stderr, "Rebuild failed: %d\n", ret_rebuild);
        return ret;
      } else return ret_rebuild;
    }
  }
#endif
  return ret;
#else
  return qf_insert(hm, key, value, flags);
  #endif
}

int hm_remove(HM *hm, uint64_t key, uint8_t flags) {
#ifdef QF_TOMBSTONE
  return qft_remove(hm, key, flags);
#else
  return qf_remove(hm, key, flags);
#endif
}

int hm_lookup(const QF *hm, uint64_t key, uint64_t *value, uint8_t flags) {
#ifdef QF_TOMBSTONE
  return qft_query(hm, key, value, flags);
#else
  return qf_lookup(hm, key, value, flags);
#endif
}

