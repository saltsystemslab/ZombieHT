/******************************************************************
 * Code for ds with tombstones.
 ******************************************************************/
#ifndef TS_UTIL_H
#define TS_UTIL_H

#include "util.h"
#include <math.h>
#include <stdio.h>

#ifdef QF_TOMBSTONE

static inline int is_tombstone(const QF *qf, uint64_t index) {
  return (METADATA_WORD(qf, tombstones, index) >>
          ((index % QF_SLOTS_PER_BLOCK) % 64)) &
         1ULL;
}

/* Find the first tombstone in [from, xnslots), it can be empty or not empty. */
static inline size_t find_next_tombstone(QF *qf, size_t from) {
  size_t block_index = from / QF_SLOTS_PER_BLOCK;
  const size_t slot_offset = from % QF_SLOTS_PER_BLOCK;
  size_t tomb_offset =
      bitselectv(get_block(qf, block_index)->tombstones[0], slot_offset, 0);
  while (tomb_offset == 64) { // No tombstone in the rest of this block.
    block_index++;
    tomb_offset = bitselect(get_block(qf, block_index)->tombstones[0], 0);
  }
  return block_index * QF_SLOTS_PER_BLOCK + tomb_offset;
}

/* Shift metadata runends and tombstones in range [first, last) to the big
 * direction by distance.
 * `last` to `last+distance-1` will be replaced. Fill with 0s in the small size.
 */
static inline void shift_runends_tombstones(QF *qf, int64_t first,
                                            uint64_t last, uint64_t distance) {
  assert(last < qf->metadata->xnslots);
  assert(distance < 64);
  uint64_t first_word = first / 64;
  uint64_t bstart = first % 64;
  uint64_t last_word = (last + distance - 1) / 64;
  uint64_t bend = (last + distance - 1) % 64 + 1;

  if (last_word != first_word) {
    METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
        METADATA_WORD(qf, runends, 64 * (last_word - 1)),
        METADATA_WORD(qf, runends, 64 * last_word), 0, bend, distance);
    METADATA_WORD(qf, tombstones, 64 * last_word) = shift_into_b(
        METADATA_WORD(qf, tombstones, 64 * (last_word - 1)),
        METADATA_WORD(qf, tombstones, 64 * last_word), 0, bend, distance);
    bend = 64;
    last_word--;
    while (last_word != first_word) {
      METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
          METADATA_WORD(qf, runends, 64 * (last_word - 1)),
          METADATA_WORD(qf, runends, 64 * last_word), 0, bend, distance);
      METADATA_WORD(qf, tombstones, 64 * last_word) = shift_into_b(
          METADATA_WORD(qf, tombstones, 64 * (last_word - 1)),
          METADATA_WORD(qf, tombstones, 64 * last_word), 0, bend, distance);
      last_word--;
    }
  }
  METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
      0, METADATA_WORD(qf, runends, 64 * last_word), bstart, bend, distance);
  METADATA_WORD(qf, tombstones, 64 * last_word) = shift_into_b(
      0, METADATA_WORD(qf, tombstones, 64 * last_word), bstart, bend, distance);
}



static inline bool is_empty_ts(const QF *qf, uint64_t slot_index) {
  // if (!is_tombstone(qf, slot_index)) return false;
  return offset_lower_bound(qf, slot_index) == 0;
}

/* Find next tombstone/empty `available` in range [index, nslots)
 * Shift everything in range [index, available) by 1 to the big direction.
 * Make a tombstone at `index`
 * Return:
 *     >=0: Distance between index and `available`.
 */
static inline int _insert_ts_at(QF *const qf, size_t index, size_t run) {
  if (is_tombstone(qf, index)) return 0;
  size_t available_slot_index = find_next_tombstone(qf, index);
  if (available_slot_index >= qf->metadata->xnslots) return QF_NO_SPACE;
  // Change counts
  if (is_empty(qf, available_slot_index))
    modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
  // shift slot and metadata
  shift_remainders(qf, index, available_slot_index);
  shift_runends_tombstones(qf, index, available_slot_index, 1);
  SET_T(qf, index);
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
  _recalculate_block_offsets(qf, index, available_slot_index);
#else
  _recalculate_block_offsets(qf, run);
#endif
  return available_slot_index - index;
}

/* Find the smallest existing run >= `run`. 
 * Insert a tombstone at the beginning of the run.
 * Return:
 *     >=0: Distance between index and `available`.
 */
static inline int _insert_pts(QF *const qf, size_t run) {
  size_t index = run_start(qf, run);
  if (index > run)  // Find the right run for maitaining the block offset.
    run = find_next_run(qf, run);
  return _insert_ts_at(qf, index, run);
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
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
    _recalculate_block_offsets(qf, curr_quotien, push_end);
#else
    _recalculate_block_offsets(qf, curr_quotien);
#endif
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
        if (ret < 0) return ret;
        push_end = push_start += 1;
        size_t n_skipped_runs = runends_cnt(grhm, push_start, ret);
        if (n_skipped_runs > 0) {
          curr_run = occupieds_select(grhm, curr_run, n_skipped_runs);
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
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
    _recalculate_block_offsets(grhm, curr_run, push_end);
#else
    _recalculate_block_offsets(grhm, curr_run);
#endif
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
  return 0;
}

/* Rebuild within 1 round. 
 * There may exists overlap between shifts for insertion consecutive tombstones.
 */
static void _rebuild_no_insertion(QF *grhm, size_t from_run, size_t until_run, size_t ts_space) {
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
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
    _recalculate_block_offsets(grhm, curr_run, push_end);
#else
    _recalculate_block_offsets(grhm, curr_run);
#endif
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

static void reset_rebuild_cd(HM *trhm) {
  if (trhm->metadata->nrebuilds != 0)
    trhm->metadata->rebuild_cd = trhm->metadata->nrebuilds;
  else {
    // n/log_b^p(x), x=1/(1-load_factor) [Graveyard paper Section 3.3]
    // TODO: Find the best rebuild_cd, try n/log_b^p(x) for p >= 1 and b >=2.
    qf_sync_counters(trhm);
    size_t nslots = trhm->metadata->nslots;
    size_t nelts = trhm->metadata->nelts;
    double x = (double)nslots / (double)(nslots - nelts);
    trhm->metadata->rebuild_cd = (int)((double)nslots/log(x+2)/log(x+2));
    fprintf(stdout, "Rebuild cd: %u\n", trhm->metadata->rebuild_cd);
  }
}

static int find(const QF *qf, const uint64_t quotient, const uint64_t remainder,
                uint64_t *const index, uint64_t *const run_start_index,
                uint64_t *const run_end_index) {
  *run_start_index = 0;
  if (quotient != 0)
    *run_start_index = run_end(qf, quotient - 1) + 1;
  *index = *run_start_index;
  if (!is_occupied(qf, quotient)) {
    // no such run
    *run_end_index = *run_start_index;
    return 0;
  }
  *run_end_index = run_end(qf, quotient) + 1;
  uint64_t curr_remainder;
  do {
    if (!is_tombstone(qf, *index)) {
      curr_remainder = get_slot(qf, *index) >> qf->metadata->value_bits;
      if (remainder == curr_remainder)
        return 1;
      if (remainder < curr_remainder)
        return 0;
    }
    *index += 1;
  } while (*index < *run_end_index);
  return 0;
}

#endif

#endif // TS_UTIL_H
