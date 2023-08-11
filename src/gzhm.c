#include "gzhm.h"
#include "util.h"
#include <stddef.h>
#include <assert.h>
#include <stdlib.h>
#include "ts_util.h"

/******************************************************************************
 * Tombstone cleaning functions.                                              *
 ******************************************************************************/

// Find the space between primitive tombstones.
static size_t _get_ts_space(GZHM *gzhm) {
  size_t ts_space = gzhm->metadata->tombstone_space;
  if (ts_space == 0) {
    // Default tombstone space: 2.5x, x=1/(1-load_factor). [Our paper]
    size_t nslots = gzhm->metadata->nslots;
    size_t nelts = gzhm->metadata->nelts;
    ts_space = (2.5 * nslots) / (nslots - nelts);
  }
  return ts_space;
}


/* Clear tombstones in range [`from_run`, `until_run`). 
 * Set `until_run` be the first run >= `untile_run`.
 * Leave the pushing tombstones at the beginning of until_run. 
 * Return the number of pushing tombstones at the end.
 */
static void _clear_tombstones_range(QF *qf, size_t from_run, size_t until_run) {
  // TODO: multi thread this.
  size_t curr_run = find_next_run(qf, from_run);
  size_t push_start = run_start(qf, curr_run);
  size_t push_end = push_start;
  while (curr_run < until_run) {
    // Range of pushing tombstones is [push_start, push_end).
    _push_over_run(qf, &push_start, &push_end);
    // fix block offset if necessary.
    _recalculate_block_offsets(qf, curr_run);
    // find the next run
    curr_run = find_next_run(qf, ++curr_run);
    if (push_start < curr_run) {  // Reached the end of the cluster.
      size_t n_to_free = MIN(curr_run, push_end) - push_start;
      if (n_to_free > 0)
        modify_metadata(&qf->runtimedata->pc_noccupied_slots, -n_to_free);
      push_start = curr_run;
      push_end = MAX(push_end, push_start);
    }
  }
}

/* Insert primitive tombstones in range [`from_run`, `until_run`). */
static int _insert_pts_range(GZHM *gzhm, size_t from_run, size_t until_run) {
  // Find the space between primitive tombstones.
  size_t ts_space = _get_ts_space(gzhm);
  size_t pts = (from_run / ts_space + 1) * ts_space - 1;
  while (pts < until_run) {
    size_t runstart = run_start(gzhm, pts);
    int ret = _insert_ts_at(gzhm, runstart);
    if (ret < 0) abort();
    pts += ts_space;
  }
  return 0;
}

/* Rebuild with 2 rounds. */
static int _rebuild_2round(GZHM *gzhm) {
  size_t rebuild_interval = gzhm->metadata->rebuild_interval;
  if (rebuild_interval == 0)
    // Default rebuild interval: 1.5(pts space) [Our paper]
    rebuild_interval = 1.5 * _get_ts_space(gzhm);
  size_t from_run = gzhm->metadata->rebuild_run;
  size_t until_run = from_run + rebuild_interval;
  gzhm->metadata->rebuild_run += rebuild_interval;
  if (gzhm->metadata->rebuild_run >= gzhm->metadata->nslots)
    gzhm->metadata->rebuild_run = 0;
  _clear_tombstones_range(gzhm, from_run, until_run);
  return _insert_pts_range(gzhm, from_run, until_run);
}

/* Start at `qf->metadata->rebuild_run`, 
 * rebuild `qf->metadata->rebuild_interval` quotiens. 
 * Leave the pushing tombstones at the beginning of the next run. 
 * Here we do rebuild run by run. 
 * Return the number of pushing tombstones at the end.
 */
static void _rebuild(QF *qf) {
  // TODO: multi thread this.
  size_t curr_quotien = qf->metadata->rebuild_run;
  size_t push_start = run_start(qf, curr_quotien);
  size_t push_end = push_start;
  while (curr_quotien < qf->metadata->nslots) {
    // Range of pushing tombstones is [push_start, push_end).
    _push_over_run(qf, &push_start, &push_end);
    // fix block offset if necessary.
    _recalculate_block_offsets(qf, curr_quotien);
    // find the next run
    curr_quotien = find_next_run(qf, ++curr_quotien);
    if (push_start < curr_quotien) {  // Reached the end of the cluster.
      size_t n_to_free = MIN(curr_quotien, push_end) - push_start;
      if (n_to_free > 0)
        modify_metadata(&qf->runtimedata->pc_noccupied_slots, -n_to_free);
      push_start = curr_quotien;
      push_end = MAX(push_end, push_start);
    }
  }
}

/******************************************************************************
 * Graveyard RobinHood HashMap.                                               *
 ******************************************************************************/

uint64_t gzhm_init(GZHM *gzhm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len) {
  abort();
}

bool gzhm_malloc(GZHM *gzhm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed) {
  bool ret = qf_malloc(gzhm, nslots, key_bits, value_bits, hash, seed);
  // reset_rebuild_cd(gzhm);
  return ret;
}

void gzhm_destroy(GZHM *gzhm) {
    abort();
}

bool gzhm_free(GZHM *gzhm) {
  return qf_free(gzhm);
}

int gzhm_insert(GZHM *gzhm, uint64_t key, uint64_t value, uint8_t flags) {
  int ret = qft_insert(gzhm, key, value, flags);
  if (ret >= 0)
    if (--(gzhm->metadata->rebuild_cd) == 0) {
      qf_sync_counters(gzhm);
      // printf("Before clear, nelts: %u, noccupied_slots: %u\n", gzhm->metadata->nelts, gzhm->metadata->noccupied_slots);
      _rebuild_2round(gzhm);
      qf_sync_counters(gzhm);
      // printf("After clear, nelts: %u, noccupied_slots: %u\n", gzhm->metadata->nelts, gzhm->metadata->noccupied_slots);
      // reset_rebuild_cd(gzhm);
    }
  return ret;
}

int gzhm_remove(GZHM *gzhm, uint64_t key, uint8_t flags) {
  return qft_remove(gzhm, key, flags);
}

int gzhm_rebuild(GZHM *gzhm, uint8_t flags) {   
  // printf("Rebuilding...\n");
}

int gzhm_lookup(const GZHM *gzhm, uint64_t key, uint64_t *value, uint8_t flags) {
  return qft_query(gzhm, key, value, flags);
}

