#include "grhm.h"
#include "util.h"
#include <stddef.h>
#include <assert.h>
#include <stdlib.h>

/******************************************************************************
 * Tombstone cleaning functions.                                              *
 ******************************************************************************/

/* Rebuild quotien [`from_run`, `until_run`). Leave the pushing tombstones at
 * the beginning of until_run. Here we do rebuild run by run. 
 * Return the number of pushing tombstones at the end.
 */
int _rebuild(QF *qf, size_t *const from_run, size_t until_run) {
  // TODO: multi thread this.
  if (!is_occupied(qf, *from_run))
    *from_run = find_next_occupied(qf, *from_run);
	size_t push_start, push_end, runstart_index;
  push_end = push_start = runstart_index = run_start(qf, *from_run);
  // Range of pushing tombstones is [push_start, push_end).
  // push over the current run, set `from_run` to the next non-empty run.
  while (*from_run < until_run) {
    if ((*from_run-1) % qf->metadata->tombstone_space == 0) {
      // Need a primitive tombstone at `from_run`.
      if (push_start == push_end) {
        int ret = _insert_ts_at(qf, runstart_index);
        if (ret == QF_NO_SPACE) return QF_NO_SPACE;
        push_end++;
      }
      push_start++;
    }
    while (!is_runend(qf, push_end-1)) {
      // push 1 slot at a time.
      if (!is_tombstone(qf, push_end)) {
        if (push_start != push_end) {
          RESET_T(qf, push_start);
          SET_T(qf, push_end);
          set_slot(qf, push_start, get_slot(qf, push_end));
        }
        ++push_start;
      }
      ++push_end;
    }
    // reached the end of the run
    // reset first, because push_start may equal to push_end.
    RESET_R(qf, push_end - 1);
    SET_R(qf, push_start - 1);
    // find the next run start
    *from_run += 1;
    if (!is_occupied(qf, *from_run))
      *from_run = find_next_occupied(qf, *from_run);
    if (push_start <= *from_run) {  // Reached the end of the cluster.
      push_start = *from_run;
      push_end = MAX(push_end, push_start);
    }
  }
  return push_end - push_start;
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
    abort();
}

void grhm_destroy(GRHM *grhm) {
    abort();
}

bool grhm_free(QF *qf) {
    abort();
}

int grhm_insert(GRHM *grhm, uint64_t key, uint64_t value, uint8_t flags) {
    abort();
}

int grhm_remove(GRHM *grhm, uint64_t key, uint8_t flags) {
    abort();
}

int grhm_rebuild(GRHM *grhm, uint8_t flags) {   
  abort();
}

int grhm_lookup(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags) {
    abort();
}

