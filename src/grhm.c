#include "grhm.h"
#include "util.h"
#include <stddef.h>
#include <assert.h>
#include <stdlib.h>
#include "ts_util.h"

/******************************************************************************
 * Tombstone cleaning functions.                                              *
 ******************************************************************************/

static void reset_rebuild_cd(GRHM *grhm) {
  if (grhm->metadata->nrebuilds != 0)
    grhm->metadata->rebuild_cd = grhm->metadata->nrebuilds;
  else {
    // n/(4x), x=1/(1-load_factor).
    qf_sync_counters(grhm);
    size_t nslots = grhm->metadata->nslots;
    size_t nelts = grhm->metadata->nelts;
    grhm->metadata->rebuild_cd = (nslots - nelts) / 4;
  }
}

// Find the space between primitive tombstones.
static size_t _get_ts_space(GRHM *grhm) {
  size_t ts_space = grhm->metadata->tombstone_space;
  if (ts_space == 0) {
    // Default tombstone space: 2x, x=1/(1-load_factor). [Graveyard paper]
    size_t nslots = grhm->metadata->nslots;
    size_t nelts = grhm->metadata->nelts;
    ts_space = (2 * nslots) / (nslots - nelts);
  }
  return ts_space;
}

/* Insert primitive tombstones. */
int _insert_pts(GRHM *grhm) {
  // Find the space between primitive tombstones.
  size_t ts_space = _get_ts_space(grhm);
  size_t pts = ts_space - 1;
  while (pts < grhm->metadata->nslots) {
    size_t runstart = run_start(grhm, pts);
    int ret = _insert_ts_at(grhm, runstart);
    if (ret < 0) abort();
    pts += ts_space;
  }
  return 0;
}

/* Rebuild with 2 rounds. */
int _rebuild_2round(GRHM *grhm) {
  _clear_tombstones(grhm);
  return _insert_pts(grhm);
}

/******************************************************************************
 * Graveyard RobinHood HashMap.                                               *
 ******************************************************************************/

uint64_t grhm_init(GRHM *grhm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len) {
  abort();
}

bool grhm_malloc(GRHM *grhm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed) {
  bool ret = qf_malloc(grhm, nslots, key_bits, value_bits, hash, seed);
  reset_rebuild_cd(grhm);
  return ret;
}

void grhm_destroy(GRHM *grhm) {
    abort();
}

bool grhm_free(GRHM *grhm) {
  return qf_free(grhm);
}

int grhm_insert(GRHM *grhm, uint64_t key, uint64_t value, uint8_t flags) {
  int ret = qft_insert(grhm, key, value, flags);
  if (ret >= 0)
    if (--(grhm->metadata->rebuild_cd) == 0) {
      qf_sync_counters(grhm);
      // printf("Before clear, nelts: %u, noccupied_slots: %u\n", grhm->metadata->nelts, grhm->metadata->noccupied_slots);
      _rebuild_2round(grhm);
      qf_sync_counters(grhm);
      // printf("After clear, nelts: %u, noccupied_slots: %u\n", grhm->metadata->nelts, grhm->metadata->noccupied_slots);
      reset_rebuild_cd(grhm);
    }
  return ret;
}

int grhm_remove(GRHM *grhm, uint64_t key, uint8_t flags) {
  return qft_remove(grhm, key, flags);
}

int grhm_rebuild(GRHM *grhm, uint8_t flags) {   
  // printf("Rebuilding...\n");
}

int grhm_lookup(const GRHM *grhm, uint64_t key, uint64_t *value, uint8_t flags) {
  return qft_query(grhm, key, value, flags);
}

