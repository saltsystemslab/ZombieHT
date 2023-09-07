#ifndef QF_H
#define QF_H

#include "gqf.h"
#include "util.h"
#include <stdlib.h>

/*
 * Header file defining internal quotient non-tombstone hashmap functions*/

// TODO: Update method documentation.
int qf_insert(HM *qf, uint64_t key, uint64_t value, uint8_t flags);
int qf_lookup(const HM *hm, uint64_t key, uint64_t *value, uint8_t flags);
int qf_remove(HM *hm, uint64_t key, uint8_t flags);


/* Implementation. TODO: Move to C file.*/

static inline int qf_insert1(QF *qf, __uint128_t hash, uint8_t runtime_lock) {
  int ret_distance = 0;
  uint64_t hash_slot_value = (hash & BITMASK(qf->metadata->bits_per_slot));
  uint64_t hash_remainder = hash_slot_value >> qf->metadata->value_bits;
  uint64_t hash_bucket_index = hash >> qf->metadata->bits_per_slot;
  uint64_t hash_bucket_block_offset = hash_bucket_index % QF_SLOTS_PER_BLOCK;
  if (GET_NO_LOCK(runtime_lock) != QF_NO_LOCK) {
    if (!qf_lock(qf, hash_bucket_index, /*small*/ true, runtime_lock))
      return QF_COULDNT_LOCK;
  }

  if (is_empty(qf, hash_bucket_index) /* might_be_empty(qf, hash_bucket_index) && runend_index == hash_bucket_index */) {
    METADATA_WORD(qf, runends, hash_bucket_index) |=
        1ULL << (hash_bucket_block_offset % 64);
    set_slot(qf, hash_bucket_index, hash_slot_value);
    METADATA_WORD(qf, occupieds, hash_bucket_index) |=
        1ULL << (hash_bucket_block_offset % 64);
    ret_distance = 0;
    modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
    modify_metadata(&qf->runtimedata->pc_nelts, 1);
  } else {
    uint64_t runend_index = run_end(qf, hash_bucket_index);
    int operation = 0; /* Insert into empty bucket */
    uint64_t insert_index = runend_index + 1;
    uint64_t new_value = hash_slot_value;
    uint64_t runstart_index =
        hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
    if (is_occupied(qf, hash_bucket_index)) {
      uint64_t current_remainder = get_slot_remainder(qf, runstart_index);
      while (current_remainder < hash_remainder && runstart_index <= runend_index) {
        runstart_index++;
        /* This may read past the end of the run, but the while loop
                 condition will prevent us from using the invalid result in
                 that case. */
        current_remainder = get_slot_remainder(qf, runstart_index);
      }
      /* If this is the first time we've inserted the new remainder,
               and it is larger than any remainder in the run. */
      if (runstart_index > runend_index) {
        operation = 1;
        insert_index = runstart_index;
        new_value = hash_slot_value;
        modify_metadata(&qf->runtimedata->pc_nelts, 1);
      /* Replace the current slot with this new hash. Don't shift anything. */
      } else if (current_remainder == hash_remainder) {
        operation = -1;
        insert_index = runstart_index;
        new_value = hash_slot_value;
        set_slot(qf, insert_index, new_value);
      /* First time we're inserting this remainder, but there are 
          are larger remainders in the run. */
      } else {
        operation = 2; /* Inserting */
        insert_index = runstart_index;
        new_value = hash_slot_value;
        modify_metadata(&qf->runtimedata->pc_nelts, 1);
      }
    } else {
        modify_metadata(&qf->runtimedata->pc_nelts, 1);
    }
    if (operation >= 0) {
      uint64_t empty_slot_index;
      int ret = find_first_empty_slot(qf, runend_index + 1, &empty_slot_index);
      if (ret < 0) return ret;
            shift_remainders(qf, insert_index, empty_slot_index);
      set_slot(qf, insert_index, new_value);
      ret_distance = insert_index - hash_bucket_index;

      shift_runends(qf, insert_index, empty_slot_index - 1, 1);

      switch (operation) {
      case 0:
        METADATA_WORD(qf, runends, insert_index) |=
            1ULL << ((insert_index % QF_SLOTS_PER_BLOCK) % 64);
        break;
      case 1:
        METADATA_WORD(qf, runends, insert_index - 1) &=
            ~(1ULL << (((insert_index - 1) % QF_SLOTS_PER_BLOCK) % 64));
        METADATA_WORD(qf, runends, insert_index) |=
            1ULL << ((insert_index % QF_SLOTS_PER_BLOCK) % 64);
        break;
      case 2:
        METADATA_WORD(qf, runends, insert_index) &=
            ~(1ULL << ((insert_index % QF_SLOTS_PER_BLOCK) % 64));
        break;
      default:
        fprintf(stderr, "Invalid operation %d\n", operation);
        abort();
      }
      METADATA_WORD(qf, occupieds, hash_bucket_index) |=
          1ULL << (hash_bucket_block_offset % 64);
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
      _recalculate_block_offsets(qf, hash_bucket_index, empty_slot_index);
#else
      /*
        * Increment the offset for each block between the hash bucket index
        * and block of the empty slot
        * */
      uint64_t i;
      for (i = hash_bucket_index / QF_SLOTS_PER_BLOCK + 1;
            i <= empty_slot_index / QF_SLOTS_PER_BLOCK; i++) {
        if (get_block(qf, i)->offset <
            BITMASK(8 * sizeof(qf->blocks[0].offset)))
          get_block(qf, i)->offset++;
        else abort();
        assert(get_block(qf, i)->offset != 0 && get_block(qf, i)->offset < 255);
      }
#endif
      modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
      modify_metadata(&qf->runtimedata->pc_nelts, 1);
    }
  }
  if (GET_NO_LOCK(runtime_lock) != QF_NO_LOCK) {
    qf_unlock(qf, hash_bucket_index, /*small*/ true);
  }
  return ret_distance;
}

int qf_insert(HM *qf, uint64_t key, uint64_t value, uint8_t flags) {
  if (qf_get_num_occupied_slots(qf) >= qf->metadata->nslots * 0.99) {
    return QF_NO_SPACE;
  }
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    fprintf(stderr, "RobinHood HM assumes key is hash for now.");
    abort();
  }
  uint64_t hash = key;
  hash = (hash<< qf->metadata->value_bits) |
                  (value & BITMASK(qf->metadata->value_bits));
  return qf_insert1(qf, hash, flags);
}

int qf_remove(HM *qf, uint64_t key, uint8_t flags) {
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    fprintf(stderr, "RobinHood HM assumes key is hash for now.");
    abort();
  }
  uint64_t hash = key;
  uint64_t hash_remainder = hash & BITMASK(qf->metadata->key_remainder_bits);
  int64_t hash_bucket_index = hash >> qf->metadata->key_remainder_bits;

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
		if (!qf_lock(qf, hash_bucket_index, /*small*/ false, flags))
			return QF_COULDNT_LOCK;
	}

  /* Empty bucket */
  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  uint64_t runstart_index =
      hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
  int only_item_in_the_run = 0;
  uint64_t current_index = runstart_index;
  uint64_t current_remainder = get_slot_remainder(qf, current_index);
  while (current_remainder < hash_remainder && !is_runend(qf, current_index)) {
    	current_index = current_index + 1;
		  current_remainder = get_slot_remainder(qf, current_index);
  }
	if (current_remainder != hash_remainder)
		return QF_DOESNT_EXIST;

  if (runstart_index == current_index && is_runend(qf, current_index))
		only_item_in_the_run = 1;
  uint64_t *p = 0x00; // The New Counter length is 0.
  int ret_numfreedslots = remove_replace_slots_and_shift_remainders_and_runends_and_offsets(qf,
																																		only_item_in_the_run,
																																		hash_bucket_index,
																																		current_index,
																																		p,
																																		0,
																																		1);
  modify_metadata(&qf->runtimedata->pc_nelts, -1);
	if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
		qf_unlock(qf, hash_bucket_index, /*small*/ false);
	}
	return ret_numfreedslots;

}

int qf_lookup(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags) {
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    fprintf(stderr, "RobinHood HM assumes key is hash for now.");
    abort();
  }
  uint64_t hash = key;
  uint64_t hash_remainder = hash & BITMASK(qf->metadata->key_remainder_bits);
  int64_t hash_bucket_index = hash >> qf->metadata->key_remainder_bits;
  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  int64_t runstart_index =
      hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
  if (runstart_index < hash_bucket_index)
    runstart_index = hash_bucket_index;

  uint64_t current_slot_value, current_index, current_remainder;
  current_index = runstart_index;
  do {
    current_slot_value = get_slot(qf, current_index);
    current_remainder = current_slot_value >> qf->metadata->value_bits;
    if (current_remainder == hash_remainder) {
      *value = current_slot_value & BITMASK(qf->metadata->value_bits);
      return (current_index - runstart_index + 1);
    }
    current_index++;
  } while (!is_runend(qf, current_index - 1));
  return QF_DOESNT_EXIST;
}

#endif

