#ifndef _GRHM_H_
#define _GRHM_H_

#include "gqf.h"
#include "gqf_int.h"
#include <inttypes.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct quotient_filter graveyard_robinhood_hashmap;
typedef graveyard_robinhood_hashmap GRHM;

uint64_t grhm_init(GRHM *grhm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len);

bool grhm_malloc(GRHM *grhm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed);

void grhm_destroy(GRHM *grhm);

bool grhm_free(QF *qf);

int grhm_insert(GRHM *grhm, uint64_t key, uint64_t value, uint8_t flags);

int grhm_remove(GRHM *grhm, uint64_t key, uint8_t flags);

int grhm_rebuild(GRHM *grhm, uint8_t flags);

int grhm_lookup(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags);

#ifdef __cplusplus
}
#endif

#endif /* _RHM_H_ */
