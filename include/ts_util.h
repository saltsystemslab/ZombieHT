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

/* Find the previous runend from this position, excluding that position. */
static inline size_t find_prev_runend(QF *qf, size_t slot) {
  size_t block_index = slot / QF_SLOTS_PER_BLOCK;
  const size_t slot_offset = slot % QF_SLOTS_PER_BLOCK;
  size_t block_runend_word = get_block(qf, block_index)->runends[0];
  // mask the higher order bits, these are positions that come after the slot.
  block_runend_word = (block_runend_word & BITMASK(slot_offset)); 
  uint64_t mask = BITMASK(slot);
  size_t prev = bitscanreverse(block_runend_word);
  if (prev != BITMASK(64)) {
    return block_index * QF_SLOTS_PER_BLOCK + prev;
  }
  do {
    block_runend_word = get_block(qf, --block_index)->runends[0];
  } while (block_runend_word == 0);
  prev = bitscanreverse(block_runend_word);
  return block_index * QF_SLOTS_PER_BLOCK + prev;
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
#ifdef DEBUG
  assert(last < qf->metadata->xnslots);
  assert(distance < 64);
#endif
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
    qf->metadata->noccupied_slots++;
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

/* Return num of tombstones in range [start, start+end). */
static inline size_t tombstones_cnt(const QF *qf, size_t start, size_t len) {
  size_t cnt = 0;
  size_t end = start + len;
  size_t block_i = start / QF_SLOTS_PER_BLOCK;
  size_t bstart = start % QF_SLOTS_PER_BLOCK;
  do {
    size_t word = get_block(qf, block_i)->tombstones[0];
    cnt += popcntv(word, bstart);
    block_i++;
    bstart = 0;
  } while ((block_i) * QF_SLOTS_PER_BLOCK <= end);
  size_t word = get_block(qf, block_i-1)->tombstones[0];
  cnt -= popcntv(word, end % QF_SLOTS_PER_BLOCK);
  return cnt;
}

/* Push tombstones over an existing run (has at least one non-tombstone).
 * Range of pushing tombstones is [push_start, push_end).
 * push_start is also the start of the run.
 * After this, push_start-1 is the end of the run.
 */
static void _push_over_run(QF *qf, size_t *push_start, size_t *push_end) {
  #ifdef MEMMOVE_PUSH
  //[runstart, runend] is the window over which we clear tombstones.
  //[push_start/runstart, *push_end) are tombstones.
  int runstart = *push_start;
  int runend = runends_select(qf, *push_end, 0);
  size_t push_len = (*push_end - *push_start);
  int next_tombstone;
  do {
    // Find first block of items to shift left.
    // These are all items until next tombstone or runend.
    next_tombstone = find_next_tombstone(qf, *push_end);
    next_tombstone = MIN(runend, next_tombstone);

    // Shift them all and update push_end and push_start.
    shift_remainders_left(qf, *push_end, next_tombstone, push_len);
    *push_end = next_tombstone + 1;
    // If we pushed over a tombstone, we need to collect it.
    if (next_tombstone < runend) {
      push_len++;
    }
  } while (next_tombstone < runend);
  *push_start = *push_end - push_len;
  // Reset metadatablocks only if there was actually shifting.
  if (push_len) {
    reset_tombstone_block(qf, runstart, *push_start-1);
    set_tombstone_block(qf, *push_start, *push_end-1);
    RESET_R(qf, *push_end - 1);
    SET_R(qf, *push_start - 1);
  }

  #else 
  // We have to shift slot by slot to collect tombstones.
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
  #endif
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
        qf->metadata->noccupied_slots -= n_to_free;
      push_start = curr_quotien;
      push_end = MAX(push_end, push_start);
    }
  }
}

#if 0
/* Insert primitive tombstones. */
static int _insert_all_pts(GRHM *grhm) {
  // Find the space between primitive tombstones.
  size_t ts_space = _get_ts_space(grhm);
  size_t pts = ts_space - 1;
  while (pts < grhm->metadata->nslots) {
    // size_t runstart = run_start(grhm, pts);
    // int ret = _insert_ts_at(grhm, runstart);
    int ret = _insert_pts(grhm, pts);
    if (ret < 0) abort();
    pts += ts_space;
  }
  return 0;
}

/* Rebuild with 2 rounds. */
static int _rebuild_2round(GRHM *grhm) {
  _clear_tombstones(grhm);
  return _insert_all_pts(grhm);
  return 0;
}
#endif


/* Rebuild within 1 round. 
 * There may exists overlap between shifts for insertion consecutive tombstones.
 */
// TODO: Maybe rename to amortized_redistribute_tombstones?
static inline int _rebuild_1round(QF *grhm, size_t from_run, size_t until_run, size_t ts_space) {
  size_t next_pts = (from_run / ts_space + 1) * ts_space - 1;  // quotient
  size_t curr_run = find_next_run(grhm, from_run);             // quotient
  size_t push_start = run_start(grhm, curr_run);  // index
  size_t push_end = push_start;                   // index
  while (curr_run < until_run) {
    if (curr_run >= next_pts) {     // Need a primitive tombstone here.
      next_pts += ts_space;
      if (push_start < push_end) {  // There are pushing tombstones.
        push_start += 1;
      } else {                      // Need to insert a new tombstone.
        int ret = _insert_ts_at(grhm, push_start, curr_run);
        if (ret < 0) return ret;    // Failed to insert one (no space).
        push_end = push_start += 1;
        size_t n_skipped_runs = runends_cnt(grhm, push_start, ret);
        if (n_skipped_runs > 0) {   // There is no tombstone in these skiped run
          curr_run = occupieds_select(grhm, curr_run, n_skipped_runs);
          if (curr_run > next_pts) 
            curr_run = find_next_run(grhm, next_pts);
          push_end = push_start = run_start(grhm, curr_run);
          continue;
        }  // Otherwise, we should continue this run.
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
        grhm->metadata->noccupied_slots -= n_to_free;
      push_start = curr_run;
      push_end = MAX(push_end, push_start);
    }
  }
  return 0;
}

/* Rebuild within 1 round. 
 * There may exists overlap between shifts for insertion consecutive tombstones.
 */
static int _rebuild_no_insertion(QF *grhm, size_t from_run, size_t until_run, size_t ts_space) {
  size_t next_pts = (from_run / ts_space + 1) * ts_space - 1;
  size_t curr_run = find_next_run(grhm, from_run);
  size_t push_start = run_start(grhm, curr_run);
  size_t push_end = push_start;
  size_t n_skipped_ts = 0;
  while (curr_run < until_run) {
    while (curr_run >= next_pts) {
      n_skipped_ts += 1;
      next_pts += ts_space;
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
        grhm->metadata->noccupied_slots -= -n_to_free;
      push_start = curr_run;
      push_end = MAX(push_end, push_start);
    }
  }
  return 0;
}

static void reset_rebuild_cd(HM *hm) {
#ifdef REBUILD_DEAMORTIZED_GRAVEYARD
  return; // Do Nothing.
#elif REBUILD_AT_INSERT
  return;
#else
  if (hm->metadata->nrebuilds != 0)
    hm->metadata->rebuild_cd = hm->metadata->nrebuilds;
  else {
    size_t nslots = hm->metadata->nslots;
    size_t nelts = hm->metadata->nelts;
#ifdef REBUILD_BY_CLEAR
    // n/log_b^p(x), x=1/(1-load_factor) [Graveyard paper Section 3.3]
    // TODO: Find the best rebuild_cd, try n/log_b^p(x) for p >= 1 and b >=2.
    double x = (double)nslots / (double)(nslots - nelts);
    hm->metadata->rebuild_cd = (int)((double)nslots/log(x+2)/log(x+2));
    // fprintf(stdout, "Rebuild cd: %u\n", hm->metadata->rebuild_cd);
#elif AMORTIZED_REBUILD
    hm->metadata->rebuild_cd = (nslots - nelts) / 4;
    // fprintf(stdout, "Rebuild cd: %u\n", hm->metadata->rebuild_cd);
#endif
  }
#endif
}

/* Given quotient and remainder, find the range of the run [start, end) and the
 * position of the slot if it exits. If it doesn't exist, the position is where
 * it should be inserted.
 * Return 1 if found, 0 otherwise.
 */
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
#ifdef UNORDERED
  uint64_t tombstone_in_run = -1;
#endif
  do {
    if (!is_tombstone(qf, *index)) {
      curr_remainder = get_slot(qf, *index) >> qf->metadata->value_bits;
      if (remainder == curr_remainder)
        return 1;
#ifndef UNORDERED
      // This is the position we need to insert at.
      if (remainder < curr_remainder)
        return 0;
#endif
    }
#ifdef UNORDERED
    else {
      // This is a tombstone we can insert at directly.
      // Still need to check if the item is already present, so don't exit just yet.
      tombstone_in_run = *index;
    }
#endif
    *index += 1;
  } while (*index < *run_end_index);
#ifdef UNORDERED
  // Check if there is a tombstone in the run you can use.
  if (tombstone_in_run != -1) {
    *index = tombstone_in_run;
  }
#endif
  return 0;
}

// Find the space between primitive tombstones.
static size_t _get_ts_space(HM *hm) {
  size_t ts_space = hm->metadata->tombstone_space;
  if (ts_space == 0) {
    // Default tombstone space: 2x, x=1/(1-load_factor). [Graveyard paper]
    size_t nslots = hm->metadata->nslots;
    size_t nelts = hm->metadata->nelts;
#if defined REBUILD_DEAMORTIZED_GRAVEYARD || defined REBUILD_AT_INSERT
    ts_space = (2.5 * nslots) / (nslots - nelts);
#elif AMORTIZED_REBUILD
    ts_space = (2 * nslots) / (nslots - nelts);
#endif
  }
  return ts_space;
}

#endif

#endif // TS_UTIL_H
