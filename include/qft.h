#ifndef QFT_H
#define QFT_H

#include "gqf.h"


#ifdef QF_TOMBSTONE

#include "ts_util.h"

int qft_insert(QF *const qf, uint64_t key, uint64_t value, uint8_t flags);
int qft_remove(HM *qf, uint64_t key, uint8_t flags);
int qft_query(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags);
void qft_rebuild(QF *qf, uint8_t flags);

int qft_insert(QF *const qf, uint64_t key, uint64_t value, uint8_t flags) {
  if (qf_get_num_occupied_slots(qf) >= qf->metadata->nslots * 0.99) {
    qft_rebuild(qf, QF_NO_LOCK);
    assert(qf_get_num_occupied_slots(qf) < qf->metadata->nslots * 0.99);
  }
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    fprintf(stderr, "RobinHood Tombstone HM assumes key is hash for now.");
    abort();
  }
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
    ret_distance = available_slot_index - hash_bucket_index + 1;
    if (available_slot_index >= qf->metadata->xnslots) {
      fprintf(stderr, "Reached xnslots.\n");
      return QF_NO_SPACE;
    }
      // return QF_NO_SPACE;
    // counts
    modify_metadata(&qf->runtimedata->pc_nelts, 1);
    if (is_empty_ts(qf, available_slot_index))
      modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
    // else use a tombstone
    // shift
    shift_remainders(qf, insert_index, available_slot_index);
    // shift_runends_tombstones(qf, insert_index, available_slot_index, 1);
    set_slot(qf, insert_index, new_value);
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
    SET_O(qf, hash_bucket_index);
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
    _clear_tombstones(hm);
    reset_rebuild_cd(hm);
}

#endif

#endif
