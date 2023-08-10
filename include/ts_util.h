/******************************************************************
 * Code for ds with tombstones.
 ******************************************************************/
#ifndef TS_UTIL_H
#define TS_UTIL_H

#include "util.h"

/* Push tombstones over an existing run (has at least one non-tombstone).
 * Range of pushing tombstones is [push_start, push_end).
 * push_start is also the start of the run.
 * After this, push_start-1 is the end of the run.
 */
static void _push_over_run(QF *qf, size_t *push_start, size_t *push_end) {
  do {
    // push 1 slot at a time.
    if (!is_tombstone(qf, *push_end)) {
      if (*push_start != *push_end) {
        RESET_T(qf, *push_start);
        SET_T(qf, *push_end);
        set_slot(qf, *push_start, get_slot(qf, *push_end));
      }
      ++*push_start;
    }
    ++*push_end;
  } while (!is_runend(qf, *push_end-1));
  // reached the end of the run
  // reset first, because push_start may equal to push_end.
  RESET_R(qf, *push_end - 1);
  SET_R(qf, *push_start - 1);
}

/* update the offset bits.
 * find the number of occupied slots in the original_bucket block.
 * Then find the runend slot corresponding to the last run in the
 * original_bucket block.
 * Update the offset of the block to which it belongs.
 */
static void _recalculate_block_offsets(QF *qf, size_t index) {
  size_t original_block = index / QF_SLOTS_PER_BLOCK;
  while (1) {
    size_t last_occupieds_hash_index =
        QF_SLOTS_PER_BLOCK * original_block + (QF_SLOTS_PER_BLOCK - 1);
    size_t runend_index = run_end(qf, last_occupieds_hash_index);
    // runend spans across the block
    // update the offset of the next block
    if (runend_index / QF_SLOTS_PER_BLOCK ==
        original_block) { // if the run ends in the same block
      if (get_block(qf, original_block + 1)->offset == 0)
        break;
      get_block(qf, original_block + 1)->offset = 0;
    } else { // if the last run spans across the block
      if (get_block(qf, original_block + 1)->offset ==
          (runend_index - last_occupieds_hash_index))
        break;
      get_block(qf, original_block + 1)->offset =
          (runend_index - last_occupieds_hash_index);
    }
    original_block++;
  }
}

#endif // TS_UTIL_H