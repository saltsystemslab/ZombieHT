#ifndef _GZHM_H_
#define _GZHM_H_

#include "gqf.h"
#include "gqf_int.h"
#include <inttypes.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct quotient_filter graveyard_zamboni_hashmap;
typedef graveyard_zamboni_hashmap GZHM;

uint64_t gzhm_init(GZHM *gzhm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len);

bool gzhm_malloc(GZHM *gzhm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed);

void gzhm_destroy(GZHM *gzhm);

bool gzhm_free(QF *qf);

int gzhm_insert(GZHM *gzhm, uint64_t key, uint64_t value, uint8_t flags);

int gzhm_remove(GZHM *gzhm, uint64_t key, uint8_t flags);

int gzhm_rebuild(GZHM *gzhm, uint8_t flags);

int gzhm_lookup(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags);

#ifdef __cplusplus
}
#endif

#endif /* _GZHM_H_ */
