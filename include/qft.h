#ifndef QFT_H
#define QFT_H

#include "gqf.h"


#ifdef QF_TOMBSTONE

#include "ts_util.h"

int qft_insert(QF *const qf, uint64_t key, uint64_t value, uint8_t flags);
int qft_remove(HM *qf, uint64_t key, uint8_t flags);
int qft_query(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags);
void qft_rebuild(QF *qf, uint8_t flags);

#ifdef REBUILD_DEAMORTIZED_GRAVEYARD
/* Start at `qf->metadata->rebuild_run`, 
 * rebuild `qf->metadata->rebuild_interval` quotients. 
 * Leave the pushing tombstones at the beginning of the next run. 
 * Here we do rebuild run by run. 
 * Return the number of pushing tombstones at the end.
 */
int _deamortized_rebuild(HM *hm) {
  size_t ts_space = _get_ts_space(hm);
  size_t rebuild_interval = hm->metadata->rebuild_interval;
  if (rebuild_interval == 0)
    // Default rebuild interval: 1.5(pts space) [Our paper]
    rebuild_interval = 1.5 * ts_space;
  // rebuild_interval /= 15;
  size_t from_run = hm->metadata->rebuild_run;
  size_t until_run = from_run + rebuild_interval;
  hm->metadata->rebuild_run = until_run;
  if (until_run >= hm->metadata->nslots) {
    until_run = hm->metadata->nslots;
    hm->metadata->rebuild_run = 0;
  }
#ifdef REBUILD_NO_INSERT
  return _rebuild_no_insertion(hm, from_run, until_run, ts_space);
#else
  return _rebuild_1round(hm, from_run, until_run, ts_space);
#endif
}
#endif

#ifdef REBUILD_AT_INSERT
/* Start at `qf->metadata->rebuild_run`, 
 * rebuild `qf->metadata->rebuild_interval` quotiens. 
 * Leave the pushing tombstones at the beginning of the next run. 
 * Here we do rebuild run by run. 
 * Return the number of pushing tombstones at the end.
 */
int _deamortized_rebuild(HM *hm, uint64_t key, uint8_t flags) {
  size_t ts_space = _get_ts_space(hm);
  size_t rebuild_interval = hm->metadata->rebuild_interval;
  if (rebuild_interval == 0)
    // Default rebuild interval: 1.5(pts space) [Our paper]
    rebuild_interval = 1.5 * ts_space;
  uint64_t hash = key2hash(hm, key, flags);
  uint64_t hash_remainder, hash_bucket_index; // remainder and quotient.
  quotien_remainder(hm, hash, &hash_bucket_index, &hash_remainder);
  size_t from_run = hash_bucket_index;
  size_t until_run = from_run + rebuild_interval;
  hm->metadata->rebuild_run = until_run;
  if (until_run >= hm->metadata->nslots) {
    until_run = hm->metadata->nslots;
    hm->metadata->rebuild_run = 0;
  }
  return _rebuild_1round(hm, from_run, until_run, ts_space);
}
#endif

int qft_insert(QF *const qf, uint64_t key, uint64_t value, uint8_t flags) {
#ifdef DEBUG
  size_t occupied_slots = qf_get_num_occupied_slots(qf);
  // printf("occupied_slots: %zu\n", occupied_slots);
  if (occupied_slots >= qf->metadata->nslots) {
    qft_rebuild(qf, QF_NO_LOCK);
    if (qf_get_num_occupied_slots(qf) >= qf->metadata->nslots) return QF_NO_SPACE;
  }
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    fprintf(stderr, "RobinHood Tombstone HM assumes key is hash for now.");
    abort();
  }
#endif
  size_t ret_distance = 0;
  uint64_t hash = key2hash(qf, key, flags);
  uint64_t hash_remainder, hash_bucket_index; // remainder and quotient.
  quotien_remainder(qf, hash, &hash_bucket_index, &hash_remainder);
  uint64_t new_value = (hash_remainder << qf->metadata->value_bits) |
                       (value & BITMASK(qf->metadata->value_bits));

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
    if (!qf_lock(qf, hash_bucket_index, /*small*/ true, flags))
      return QF_COULDNT_LOCK;
  }

  if (is_empty_ts(qf, hash_bucket_index)) {
    set_slot(qf, hash_bucket_index, new_value);
    SET_R(qf, hash_bucket_index);
    SET_O(qf, hash_bucket_index);
    RESET_T(qf, hash_bucket_index);

    modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
    modify_metadata(&qf->runtimedata->pc_nelts, 1);
  } else {
    uint64_t insert_index, runstart_index, runend_index;
    int ret = find(qf, hash_bucket_index, hash_remainder, &insert_index,
                   &runstart_index, &runend_index);
    if (ret == 1)
      return QF_KEY_EXISTS;
    uint64_t available_slot_index = find_next_tombstone(qf, insert_index);
    if (available_slot_index >= qf->metadata->xnslots)
      return QF_NO_SPACE;
    if (is_empty_ts(qf, available_slot_index))
      modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
    // shift
    shift_remainders(qf, insert_index, available_slot_index);
    // Fix metadata
    if (!is_occupied(qf, hash_bucket_index)) {
      // If it is a new run, we need a new runend
      shift_runends_tombstones(qf, insert_index, available_slot_index, 1);
      SET_R(qf, insert_index);
    } else if (insert_index >= runend_index) {
      // insert to the end of the run
      shift_runends_tombstones(qf, insert_index - 1, available_slot_index, 1);
    } else {
      // insert to the begin or middle
      shift_runends_tombstones(qf, insert_index, available_slot_index, 1);
    }
    RESET_T(qf, available_slot_index);
    set_slot(qf, insert_index, new_value);
    SET_O(qf, hash_bucket_index);
    // counts
    modify_metadata(&qf->runtimedata->pc_nelts, 1);
    // else use a tombstone
    ret_distance = available_slot_index - hash_bucket_index + 1;
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
    _recalculate_block_offsets(qf, hash_bucket_index, available_slot_index);
#else
    _recalculate_block_offsets(qf, hash_bucket_index);
#endif
  }

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
    qf_unlock(qf, hash_bucket_index, /*small*/ true);
  }
  return ret_distance;
}

int qft_insert_unordered(QF *const qf, uint64_t key, uint64_t value, uint8_t flags) {
#ifdef DEBUG
  size_t occupied_slots = qf_get_num_occupied_slots(qf);
  // printf("occupied_slots: %zu\n", occupied_slots);
  if (occupied_slots >= qf->metadata->nslots) {
    qft_rebuild(qf, QF_NO_LOCK);
    if (qf_get_num_occupied_slots(qf) >= qf->metadata->nslots) return QF_NO_SPACE;
  }
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    fprintf(stderr, "RobinHood Tombstone HM assumes key is hash for now.");
    abort();
  }
#endif
  size_t ret_distance = 0;
  uint64_t hash = key2hash(qf, key, flags);
  uint64_t hash_remainder, hash_bucket_index; // remainder and quotient.
  quotien_remainder(qf, hash, &hash_bucket_index, &hash_remainder);
  uint64_t new_value = (hash_remainder << qf->metadata->value_bits) |
                       (value & BITMASK(qf->metadata->value_bits));

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
    if (!qf_lock(qf, hash_bucket_index, /*small*/ true, flags))
      return QF_COULDNT_LOCK;
  }

  if (is_empty_ts(qf, hash_bucket_index)) {
    set_slot(qf, hash_bucket_index, new_value);
    SET_R(qf, hash_bucket_index);
    SET_O(qf, hash_bucket_index);
    RESET_T(qf, hash_bucket_index);

    modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
    modify_metadata(&qf->runtimedata->pc_nelts, 1);
  } else {
    uint64_t insert_index, runstart_index, runend_index;
    int ret = find(qf, hash_bucket_index, hash_remainder, &insert_index,
                   &runstart_index, &runend_index);
    if (ret == 1)
      return QF_KEY_EXISTS;
    uint64_t available_slot_index = find_next_tombstone(qf, runstart_index);
    if (available_slot_index < runend_index) {
      insert_index = available_slot_index;
    } else {
      if (is_empty_ts(qf, available_slot_index))
        modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
      // Move the tombstone to the end of the run.
      if (available_slot_index > runend_index) {
        size_t to_block_i = runend_index / QF_SLOTS_PER_BLOCK;
        size_t to_block_offset = runend_index % QF_SLOTS_PER_BLOCK;
        size_t from_block_i = available_slot_index / QF_SLOTS_PER_BLOCK;
        size_t from_block_offset = available_slot_index % QF_SLOTS_PER_BLOCK;
        while (to_block_i < from_block_i) {
          size_t block_runends = get_block(qf, from_block_i)->runends[0];
          block_runends &= ~BITMASK(from_block_offset);
          // TODO
        }
      }
    }
    RESET_T(qf, available_slot_index);
    set_slot(qf, insert_index, new_value);
    SET_O(qf, hash_bucket_index);
    // counts
    modify_metadata(&qf->runtimedata->pc_nelts, 1);
    // else use a tombstone
    ret_distance = available_slot_index - hash_bucket_index + 1;
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
    _recalculate_block_offsets(qf, hash_bucket_index, available_slot_index);
#else
    _recalculate_block_offsets(qf, hash_bucket_index);
#endif
  }

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
    qf_unlock(qf, hash_bucket_index, /*small*/ true);
  }
  return ret_distance;
}

int qft_remove(HM *qf, uint64_t key, uint8_t flags) {
  uint64_t hash = key2hash(qf, key, flags);
  uint64_t hash_remainder, hash_bucket_index;
  quotien_remainder(qf, hash, &hash_bucket_index, &hash_remainder);

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
		if (!qf_lock(qf, hash_bucket_index, /*small*/ false, flags))
			return QF_COULDNT_LOCK;
	}

  /* Empty bucket */
  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  uint64_t current_index, runstart_index, runend_index;
  int ret = find(qf, hash_bucket_index, hash_remainder, &current_index,
                 &runstart_index, &runend_index);
  // remainder not found
  if (ret == 0)
    return QF_DOESNT_EXIST;
  
  SET_T(qf, current_index);
	modify_metadata(&qf->runtimedata->pc_nelts, -1);

  // Make sure that the run never end with a tombstone.
  while (is_runend(qf, current_index) && is_tombstone(qf, current_index)) {
    RESET_R(qf, current_index);
    // if it is the only element in the run
    if (current_index - runstart_index == 0) {
      RESET_O(qf, hash_bucket_index);
      if (is_empty_ts(qf, current_index))
        modify_metadata(&qf->runtimedata->pc_noccupied_slots, -1);
      break;
    } else {
      SET_R(qf, current_index-1);
      if (is_empty_ts(qf, current_index))
        modify_metadata(&qf->runtimedata->pc_noccupied_slots, -1);
      --current_index;
    }
  }
  // fix block offset if necessary
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
  _recalculate_block_offsets(qf, hash_bucket_index, runend_index);
#else
  _recalculate_block_offsets(qf, hash_bucket_index);
#endif

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
    qf_unlock(qf, hash_bucket_index, /*small*/ false);
  }

  return current_index - runstart_index + 1;
}

/* Assume tombstone is in a run, push it to the end of the run.
 * Return the index of the new tombstone (end of the run).
 */
size_t _push_tombstone_to_run_end(HM *qf, size_t tombstone_index) {
  RESET_T(qf, tombstone_index);
  while (!is_runend(qf, tombstone_index)) {
    // push 1 slot at a time.
    set_slot(qf, tombstone_index, get_slot(qf, tombstone_index+1));
    tombstone_index++;
  }
  SET_T(qf, tombstone_index);
  return tombstone_index;
}

int qft_remove_push(HM *qf, uint64_t key, uint8_t flags) {
  uint64_t hash = key2hash(qf, key, flags);
  uint64_t hash_remainder, hash_bucket_index;
  quotien_remainder(qf, hash, &hash_bucket_index, &hash_remainder);

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
		if (!qf_lock(qf, hash_bucket_index, /*small*/ false, flags))
			return QF_COULDNT_LOCK;
	}

  /* Empty bucket */
  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  uint64_t current_index, runstart_index, runend_index;
  int ret = find(qf, hash_bucket_index, hash_remainder, &current_index,
                 &runstart_index, &runend_index);
  // remainder not found
  if (ret == 0)
    return QF_DOESNT_EXIST;
  
  SET_T(qf, current_index);
	modify_metadata(&qf->runtimedata->pc_nelts, -1);

  current_index = _push_tombstone_to_run_end(qf, current_index);
  RESET_R(qf, current_index);
  if (current_index == runstart_index)
    // removing the only element in the run
    RESET_O(qf, hash_bucket_index);
  else if (is_tombstone(qf, current_index-1))
    // the only other element in the run is a primitive tombstone
    RESET_O(qf, hash_bucket_index);
  else
    SET_R(qf, current_index-1);

  // fix block offset if necessary
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
  _recalculate_block_offsets(qf, hash_bucket_index, runend_index);
#else
  _recalculate_block_offsets(qf, hash_bucket_index);
#endif

  size_t ts_space = _get_ts_space(qf);
  // Now the current_index is the new ts. 
  // It is either the first one of the next run or became an empty slot.
  size_t next_pts = ((hash_bucket_index+1) / ts_space + 1) * ts_space - 1;
  size_t curr_run = find_next_run(qf, hash_bucket_index+1);
  while (current_index >= curr_run) {  // still in the cluster
    if (curr_run >= next_pts) {
      if (is_tombstone(qf, current_index+1)) {
        // There is already a primitive tombstone.
        current_index++;
        next_pts += ts_space;
      } else {
        // Leave this ts as a primitive tombstone.
        break;
      }
    }
    current_index = _push_tombstone_to_run_end(qf, current_index);
    RESET_R(qf, current_index);
    SET_R(qf, current_index-1);
    _recalculate_block_offsets(qf, curr_run, current_index);
    curr_run = find_next_run(qf, curr_run+1);
  }  // Otherwise, we reached the end of the cluster.
  if (current_index < curr_run)
    modify_metadata(&qf->runtimedata->pc_noccupied_slots, -1);

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
    qf_unlock(qf, hash_bucket_index, /*small*/ false);
  }

  return current_index - runstart_index + 1;
}

int qft_query(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags) {
  uint64_t hash = key2hash(qf, key, flags);
  uint64_t hash_remainder, hash_bucket_index;
  quotien_remainder(qf, hash, &hash_bucket_index, &hash_remainder);

  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  uint64_t current_index, runstart_index, runend_index;
  int ret = find(qf, hash_bucket_index, hash_remainder, &current_index,
                 &runstart_index, &runend_index);
  if (ret == 0)
    return QF_DOESNT_EXIST;
  *value = get_slot(qf, current_index) & BITMASK(qf->metadata->value_bits);
  return 0;
}



void qft_rebuild(QF *hm, uint8_t flags) {
#ifdef REBUILD_BY_CLEAR
    _clear_tombstones(hm);
    reset_rebuild_cd(hm);
#elif AMORTIZED_REBUILD
		size_t ts_space = _get_ts_space(hm);
#ifdef REBUILD_NO_INSERT
		int ret = _rebuild_1round(hm, 0, hm->metadata->nslots, ts_space);
#else
		int ret = _rebuild_no_insertion(hm, 0, hm->metadata->nslots, ts_space);
#endif
		reset_rebuild_cd(hm);
#elif REBUILD_DEAMORTIZED_GRAVEYARD
		abort();
#endif
}

#endif

#endif
