/******************************************************************
 * Code for ds with tombstones.
 ******************************************************************/
#ifndef TS_UTIL_H
#define TS_UTIL_H

#include "util.h"

/* Find next tombstone/empty `available` in range [index, nslots)
 * Shift everything in range [index, available) by 1 to the big direction.
 * Make a tombstone at `index`
 * Return:
 *     >=0: Distance between index and `available`.
 */
static inline int _insert_ts_at(QF *const qf, size_t index, size_t run) {
  if (is_tombstone(qf, index)) return 0;
  size_t available_slot_index = find_next_tombstone(qf, index);
  // TODO: Handle return code correctly.
  if (available_slot_index >= qf->metadata->xnslots) return QF_NO_SPACE;
  // Change counts
  if (is_empty(qf, available_slot_index))
    modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
  // shift slot and metadata
  shift_remainders(qf, index, available_slot_index);
  shift_runends_tombstones(qf, index, available_slot_index, 1);
  SET_T(qf, index);
  _recalculate_block_offsets(qf, run);
  /* Increment the offset for each block between the hash bucket index
   * and block of the empty slot  
   */
  // uint64_t i;
  // for (i = index / QF_SLOTS_PER_BLOCK;
  //       i <= available_slot_index / QF_SLOTS_PER_BLOCK; i++) {
  //   uint8_t *block_offset = &(get_block(qf, i)->offset);
  //   if (i > 0 && i * QF_SLOTS_PER_BLOCK + *block_offset -1 >= index &&
  //     i * QF_SLOTS_PER_BLOCK + *block_offset <= available_slot_index) {
  //     if (*block_offset < BITMASK(8 * sizeof(qf->blocks[0].offset)))
  //       *block_offset += 1;
  //     else abort();
  //   }
  // }
  return available_slot_index - index;
}

/* Find next tombstone/empty `available` in range [index, nslots)
 * Shift everything in range [index, available) by 1 to the big direction.
 * Make a tombstone at `index`
 * Return:
 *     >=0: Distance between index and `available`.
 */
static inline int _insert_ts(QF *const qf, size_t run) {
  size_t index = run_start(qf, run);
  if (is_tombstone(qf, index)) return 0;
  size_t available_slot_index = find_next_tombstone(qf, index);
  // TODO: Handle return code correctly.
  if (available_slot_index >= qf->metadata->xnslots) return QF_NO_SPACE;
  // Change counts
  if (is_empty(qf, available_slot_index))
    modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
  // shift slot and metadata
  shift_remainders(qf, index, available_slot_index);
  shift_runends_tombstones(qf, index, available_slot_index, 1);
  SET_T(qf, index);
  _recalculate_block_offsets(qf, run);
  return available_slot_index - index;
}


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

/* Push tombstones over an existing run (has at least one non-tombstone).
 * Range of pushing tombstones is [push_start, push_end).
 * push_start is also the start of the run.
 * After this, push_start-1 is the end of the run.
 */
static void _push_over_run_skip_ts(QF *qf, size_t *push_start, size_t *push_end,
                                   size_t *n_skipped_ts) {
  do {
    // push 1 slot at a time.
    if (!is_tombstone(qf, *push_end)) {
      if (*push_start != *push_end) {
        RESET_T(qf, *push_start);
        SET_T(qf, *push_end);
        set_slot(qf, *push_start, get_slot(qf, *push_end));
      }
      ++*push_start;
    } else if (*n_skipped_ts > 0) {
      --*n_skipped_ts;
      ++*push_start;
    }
    ++*push_end;
  } while (!is_runend(qf, *push_end-1));
  // reached the end of the run
  // reset first, because push_start may equal to push_end.
  RESET_R(qf, *push_end - 1);
  SET_R(qf, *push_start - 1);
}


/* Rebuild quotien [`from_run`, `until_run`). Leave the pushing tombstones at
 * the beginning of until_run. Here we do rebuild run by run. 
 * Return the number of pushing tombstones at the end.
 */
static void _clear_tombstones(QF *qf) {
  // TODO: multi thread this.
  size_t curr_quotien = find_next_run(qf, 0);
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


/* Rebuild within 1 round. 
 * There may exists overlap between shifts for insertion consecutive tombstones.
 */
static int _rebuild_1round(QF *grhm, size_t from_run, size_t until_run, size_t ts_space) {
  size_t pts = (from_run / ts_space + 1) * ts_space - 1;
  size_t curr_run = find_next_run(grhm, from_run);
  size_t push_start = run_start(grhm, curr_run);
  size_t push_end = push_start;
  while (curr_run < until_run) {
    if (curr_run >= pts) {
      pts += ts_space;
      if (push_start < push_end) {
        push_start += 1;
      } else {
        int ret = _insert_ts_at(grhm, push_start, curr_run);
        push_end = push_start += 1;
        if (ret < 0) abort();
        size_t n_skipped_runs = runends_cnt(grhm, push_start, ret);
        if (n_skipped_runs > 0) {
          curr_run = occupieds_rank(grhm, curr_run, n_skipped_runs);
          if (curr_run > pts) 
            curr_run = find_next_run(grhm, pts);
          push_end = push_start = run_start(grhm, curr_run);
          continue;
        }
      }
    }
    // Range of pushing tombstones is [push_start, push_end).
    _push_over_run(grhm, &push_start, &push_end);
    // fix block offset if necessary.
    _recalculate_block_offsets(grhm, curr_run);
    // find the next run
    curr_run = find_next_run(grhm, ++curr_run);
    if (push_start < curr_run) {  // Reached the end of the cluster.
      size_t n_to_free = MIN(curr_run, push_end) - push_start;
      if (n_to_free > 0)
        modify_metadata(&grhm->runtimedata->pc_noccupied_slots, -n_to_free);
      push_start = curr_run;
      push_end = MAX(push_end, push_start);
    }
  }
}


/* Rebuild within 1 round. 
 * There may exists overlap between shifts for insertion consecutive tombstones.
 */
static int _rebuild_no_insertion(QF *grhm, size_t from_run, size_t until_run, size_t ts_space) {
  size_t pts = (from_run / ts_space + 1) * ts_space - 1;
  size_t curr_run = find_next_run(grhm, from_run);
  size_t push_start = run_start(grhm, curr_run);
  size_t push_end = push_start;
  size_t n_skipped_ts = 0;
  while (curr_run < until_run) {
    if (curr_run >= pts) {
      n_skipped_ts += 1;
      while (curr_run >= pts) pts += ts_space;
    }
    // Range of pushing tombstones is [push_start, push_end).
    _push_over_run_skip_ts(grhm, &push_start, &push_end, &n_skipped_ts);
    // fix block offset if necessary.
    _recalculate_block_offsets(grhm, curr_run);
    // find the next run
    curr_run = find_next_run(grhm, ++curr_run);
    if (push_start < curr_run) {  // Reached the end of the cluster.
      size_t n_to_free = MIN(curr_run, push_end) - push_start;
      if (n_to_free > 0)
        modify_metadata(&grhm->runtimedata->pc_noccupied_slots, -n_to_free);
      push_start = curr_run;
      push_end = MAX(push_end, push_start);
    }
  }
}

#endif // TS_UTIL_H