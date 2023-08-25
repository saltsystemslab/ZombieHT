#ifndef _HM_H_
#define _HM_H_

#include "gqf.h"
#include "gqf_int.h"
#include <inttypes.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct quotient_filter hashmap;
typedef hashmap HM;

uint64_t hm_init(HM *hm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len);

bool hm_malloc(HM *hm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed);

void hm_destroy(HM *hm);

bool hm_free(QF *qf);

int hm_insert(HM *hm, uint64_t key, uint64_t value, uint8_t flags);

int hm_remove(HM *hm, uint64_t key, uint8_t flags);

int hm_lookup(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags);

int hm_rebuild(const QF *qf, uint8_t flags);

#ifdef __cplusplus
}
#endif

#endif /* _HM_H_ */
