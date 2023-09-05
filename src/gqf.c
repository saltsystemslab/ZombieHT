/*
 * ============================================================================
 *
 *        Authors:  Prashant Pandey <ppandey@cs.stonybrook.edu>
 *                  Rob Johnson <robj@vmware.com>
 *                  Rob Johnson <robj@vmware.com>
 *
 * ============================================================================
 */

#include <assert.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <time.h>
#include <unistd.h>

#include "gqf.h"
#include "hm.h"
#include "gqf_int.h"
#include "hashutil.h"
#include "util.h"
#include "ts_util.h"

void qf_dump_metadata(const QF *qf) {
  printf("Slots: %lu Occupied: %lu Elements: %lu\n", qf->metadata->nslots,
         qf->metadata->noccupied_slots, qf->metadata->nelts);
  printf(
      "Key_bits: %lu Value_bits: %lu Remainder_bits: %lu Bits_per_slot: %lu\n",
      qf->metadata->key_bits, qf->metadata->value_bits,
      qf->metadata->key_remainder_bits, qf->metadata->bits_per_slot);
}

void qf_dump(const QF *qf) {
  uint64_t i;

  printf("nblocks: %lu; nelts: %lu.\n", qf->metadata->nblocks,
         qf->metadata->nelts);

  for (i = 0; i < qf->metadata->nblocks; i++) {
    qf_dump_block(qf, i);
  }
}
void qf_dump_long(const QF *qf) {
  uint64_t i;

  printf("%lu %lu\n", qf->metadata->nblocks, qf->metadata->nelts);

  for (i = 0; i < qf->metadata->nblocks; i++) {
    qf_dump_block_long(qf, i);
  }
  for (i = 0; i < qf->metadata->nblocks; i++) {
    qf_dump_block(qf, i);
  }
}


/*****************************************************************************
 * Code that uses the above to implement key-value operations.               *
 *****************************************************************************/

/* TODO: If tombstone_space == 0 and/or nrebuilds == 0, automaticlly calculate
 * them based on current load factor when rebuiding. */
uint64_t qf_init_advanced(QF *qf, uint64_t nslots, uint64_t key_bits,
                          uint64_t value_bits, uint64_t tombstone_space,
                          uint64_t nrebuilds, enum qf_hashmode hash,
                          uint32_t seed, void *buffer, uint64_t buffer_len) {
  uint64_t num_slots, xnslots, nblocks;
  uint64_t key_remainder_bits, bits_per_slot;
  uint64_t size;
  uint64_t total_num_bytes;
  // number of partition counters and the count threshold
  uint32_t num_counters = 8, threshold = 100;

  assert(popcnt(nslots) == 1); /* nslots must be a power of 2 */
  num_slots = nslots;
  xnslots = nslots + 10 * sqrt((double)nslots);
  nblocks = (xnslots + QF_SLOTS_PER_BLOCK - 1) / QF_SLOTS_PER_BLOCK;
  key_remainder_bits = key_bits;
  // set remainder_bits = key_bits - size_bits, where size_bits = log2(nslots)
  while (nslots > 1 && key_remainder_bits > 0) {
    key_remainder_bits--;
    nslots >>= 1;
  }
  // TODO: Why?
  assert(key_remainder_bits >= 2);

  bits_per_slot = key_remainder_bits + value_bits;
  assert(QF_BITS_PER_SLOT == 0 ||
         QF_BITS_PER_SLOT == qf->metadata->bits_per_slot);
  assert(bits_per_slot > 1);
#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 ||                         \
    QF_BITS_PER_SLOT == 32 || QF_BITS_PER_SLOT == 64
  size = nblocks * sizeof(qfblock);
#else
  size = nblocks * (sizeof(qfblock) + QF_SLOTS_PER_BLOCK * bits_per_slot / 8);
#endif

  total_num_bytes = sizeof(qfmetadata) + size;
  if (buffer == NULL || total_num_bytes > buffer_len)
    return total_num_bytes;

  memset(buffer, 0, total_num_bytes);
  qf->metadata = (qfmetadata *)(buffer);
  qf->blocks = (qfblock *)(qf->metadata + 1);

  qf->metadata->magic_endian_number = MAGIC_NUMBER;
  qf->metadata->reserved = 0;
  qf->metadata->hash_mode = hash;
  qf->metadata->total_size_in_bytes = size;
  qf->metadata->seed = seed;
  qf->metadata->nslots = num_slots;
  qf->metadata->xnslots = xnslots;
#ifdef QF_TOMBSTONE
  qf->metadata->rebuild_run = 0;
  qf->metadata->tombstone_space = tombstone_space;
  qf->metadata->nrebuilds = nrebuilds;
  qf->metadata->rebuild_interval =
      nrebuilds == 0 ? 0 : ((xnslots / nrebuilds) + 1);
  qf->metadata->rebuild_cd = nrebuilds;
#endif
  qf->metadata->key_bits = key_bits;
  qf->metadata->value_bits = value_bits;
  qf->metadata->key_remainder_bits = key_remainder_bits;
  qf->metadata->bits_per_slot = bits_per_slot;

  qf->metadata->range = qf->metadata->nslots;
  qf->metadata->range <<= qf->metadata->key_remainder_bits;
  qf->metadata->nblocks =
      (qf->metadata->xnslots + QF_SLOTS_PER_BLOCK - 1) / QF_SLOTS_PER_BLOCK;
  // qf->metadata->rebuild_pos = 0;
  // qf->metadata->next_tombstone = qf->metadata->tombstone_space;
  qf->metadata->nelts = 0;
  qf->metadata->noccupied_slots = 0;

#ifdef QF_TOMBSTONE
  // Set all tombstones
  char *b = (char *)(qf->blocks);
  size_t block_size =
      sizeof(qfblock) + QF_SLOTS_PER_BLOCK * qf->metadata->bits_per_slot / 8;
  for (uint64_t i = 0; i < qf->metadata->nblocks; i++) {
    ((qfblock *)b)->tombstones[0] = 0xffffffffffffffffULL;
    b += block_size;
  }
#endif

  qf->runtimedata->num_locks = (qf->metadata->xnslots / NUM_SLOTS_TO_LOCK) + 2;

  pc_init(&qf->runtimedata->pc_nelts, (int64_t *)&qf->metadata->nelts,
          num_counters, threshold);
  pc_init(&qf->runtimedata->pc_noccupied_slots,
          (int64_t *)&qf->metadata->noccupied_slots, num_counters, threshold);
#ifdef QF_TOMBSTONE
  pc_init(&qf->runtimedata->pc_rebuild_cd,
          (int64_t *)&qf->metadata->rebuild_cd, num_counters, threshold);
#endif
  /* initialize container resize */
  qf->runtimedata->auto_resize = 0;
  /* initialize all the locks to 0 */
  qf->runtimedata->metadata_lock = 0;
  qf->runtimedata->locks =
      (volatile int *)calloc(qf->runtimedata->num_locks, sizeof(volatile int));
  if (qf->runtimedata->locks == NULL) {
    perror("Couldn't allocate memory for runtime locks.");
    exit(EXIT_FAILURE);
  }
#ifdef LOG_WAIT_TIME
  qf->runtimedata->wait_times = (wait_time_data *)calloc(
      qf->runtimedata->num_locks + 1, sizeof(wait_time_data));
  if (qf->runtimedata->wait_times == NULL) {
    perror("Couldn't allocate memory for runtime wait_times.");
    exit(EXIT_FAILURE);
  }
#endif

  return total_num_bytes;
}

uint64_t qf_init(QF *qf, uint64_t nslots, uint64_t key_bits,
                 uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                 void *buffer, uint64_t buffer_len) {
  return qf_init_advanced(qf, nslots, key_bits, value_bits, 0, 0, hash, seed,
                          buffer, buffer_len);
}

uint64_t qf_use(QF *qf, void *buffer, uint64_t buffer_len) {
  qf->metadata = (qfmetadata *)(buffer);
  if (qf->metadata->total_size_in_bytes + sizeof(qfmetadata) > buffer_len) {
    return qf->metadata->total_size_in_bytes + sizeof(qfmetadata);
  }
  qf->blocks = (qfblock *)(qf->metadata + 1);

  qf->runtimedata = (qfruntime *)calloc(sizeof(qfruntime), 1);
  if (qf->runtimedata == NULL) {
    perror("Couldn't allocate memory for runtime data.");
    exit(EXIT_FAILURE);
  }
  /* initialize all the locks to 0 */
  qf->runtimedata->metadata_lock = 0;
  qf->runtimedata->locks =
      (volatile int *)calloc(qf->runtimedata->num_locks, sizeof(volatile int));
  if (qf->runtimedata->locks == NULL) {
    perror("Couldn't allocate memory for runtime locks.");
    exit(EXIT_FAILURE);
  }
#ifdef LOG_WAIT_TIME
  qf->runtimedata->wait_times = (wait_time_data *)calloc(
      qf->runtimedata->num_locks + 1, sizeof(wait_time_data));
  if (qf->runtimedata->wait_times == NULL) {
    perror("Couldn't allocate memory for runtime wait_times.");
    exit(EXIT_FAILURE);
  }
#endif

  return sizeof(qfmetadata) + qf->metadata->total_size_in_bytes;
}

void *qf_destroy(QF *qf) {
  assert(qf->runtimedata != NULL);
  if (qf->runtimedata->locks != NULL)
    free((void *)qf->runtimedata->locks);
  if (qf->runtimedata->wait_times != NULL)
    free(qf->runtimedata->wait_times);
  free(qf->runtimedata);

  return (void *)qf->metadata;
}

bool qf_malloc(QF *qf, uint64_t nslots, uint64_t key_bits, uint64_t value_bits,
               enum qf_hashmode hash, uint32_t seed, float max_load_factor) {
  size_t tombstone_space = 0, nrebuilds = 0;
  float x = 1.0 / (1.0 - max_load_factor);
#ifdef AMORTIZED_REBUILD
  tombstone_space = 2 * x;
#elif defined REBUILD_DEAMORTIZED_GRAVEYARD || defined REBUILD_AT_INSERT \
    || defined DELETE_AND_PUSH
  tombstone_space = 2.5 * x;
#endif
  return qf_malloc_advance(qf, nslots, key_bits, value_bits, hash, seed,
                           tombstone_space, nrebuilds);
}

bool qf_malloc_advance(QF *qf, uint64_t nslots, uint64_t key_bits,
                       uint64_t value_bits, enum qf_hashmode hash,
                       uint32_t seed, uint64_t tombstone_space,
                       uint64_t nrebuilds) {
  uint64_t total_num_bytes =
      qf_init_advanced(qf, nslots, key_bits, value_bits, tombstone_space,
                       nrebuilds, hash, seed, NULL, 0);

  void *buffer = malloc(total_num_bytes);
  if (buffer == NULL) {
    perror("Couldn't allocate memory for the CQF.");
    exit(EXIT_FAILURE);
  }

  qf->runtimedata = (qfruntime *)calloc(sizeof(qfruntime), 1);
  if (qf->runtimedata == NULL) {
    perror("Couldn't allocate memory for runtime data.");
    exit(EXIT_FAILURE);
  }

  uint64_t init_size =
      qf_init_advanced(qf, nslots, key_bits, value_bits, tombstone_space,
                       nrebuilds, hash, seed, buffer, total_num_bytes);

  if (init_size == total_num_bytes)
    return true;
  else
    return false;
}

bool qf_free(QF *qf) {
  assert(qf->metadata != NULL);
  void *buffer = qf_destroy(qf);
  if (buffer != NULL) {
    free(buffer);
    return true;
  }

  return false;
}

void qf_copy(QF *dest, const QF *src) {
  DEBUG_CQF("%s\n", "Source CQF");
  DEBUG_DUMP(src);
  memcpy(dest->runtimedata, src->runtimedata, sizeof(qfruntime));
  memcpy(dest->metadata, src->metadata, sizeof(qfmetadata));
  memcpy(dest->blocks, src->blocks, src->metadata->total_size_in_bytes);
  DEBUG_CQF("%s\n", "Destination CQF after copy.");
  DEBUG_DUMP(dest);
}

void qf_reset(QF *qf) {
  qf->metadata->nelts = 0;
  qf->metadata->noccupied_slots = 0;

#ifdef LOG_WAIT_TIME
  memset(qf->wait_times, 0,
         (qf->runtimedata->num_locks + 1) * sizeof(wait_time_data));
#endif
#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 ||                         \
    QF_BITS_PER_SLOT == 32 || QF_BITS_PER_SLOT == 64
  memset(qf->blocks, 0, qf->metadata->nblocks * sizeof(qfblock));
#else
  memset(qf->blocks, 0,
         qf->metadata->nblocks *
             (sizeof(qfblock) +
              QF_SLOTS_PER_BLOCK * qf->metadata->bits_per_slot / 8));
#endif
}

uint64_t qf_get_key_from_index(const QF *qf, const size_t index) {
  return get_slot(qf, index) >> qf->metadata->value_bits;
}

int64_t qf_get_unique_index(const QF *qf, uint64_t key, uint64_t value,
                            uint8_t flags) {
  if (GET_KEY_HASH(flags) == QF_HASH_INVERTIBLE)
    key = hash_64(key, BITMASK(qf->metadata->key_bits));

  uint64_t hash = (key << qf->metadata->value_bits) |
                  (value & BITMASK(qf->metadata->value_bits));
  uint64_t hash_remainder = hash & BITMASK(qf->metadata->bits_per_slot);
  int64_t hash_bucket_index = hash >> qf->metadata->bits_per_slot;

  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  int64_t runstart_index =
      hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
  if (runstart_index < hash_bucket_index)
    runstart_index = hash_bucket_index;

  /* printf("MC RUNSTART: %02lx RUNEND: %02lx\n", runstart_index, runend_index);
   */

  uint64_t current_remainder, current_end;
  do {
    current_end = runstart_index;
    current_remainder = get_slot(qf, current_end);
    if (current_remainder == hash_remainder)
      return runstart_index;

    runstart_index = current_end + 1;
  } while (!is_runend(qf, current_end));

  return QF_DOESNT_EXIST;
}

enum qf_hashmode qf_get_hashmode(const QF *qf) {
  return qf->metadata->hash_mode;
}
uint64_t qf_get_hash_seed(const QF *qf) { return qf->metadata->seed; }
__uint128_t qf_get_hash_range(const QF *qf) { return qf->metadata->range; }

uint64_t qf_get_total_size_in_bytes(const QF *qf) {
  return qf->metadata->total_size_in_bytes;
}
uint64_t qf_get_nslots(const QF *qf) { return qf->metadata->nslots; }
uint64_t qf_get_num_occupied_slots(const QF *qf) {
  pc_sync(&qf->runtimedata->pc_noccupied_slots);
  return qf->metadata->noccupied_slots;
}

uint64_t qf_get_num_key_bits(const QF *qf) { return qf->metadata->key_bits; }
uint64_t qf_get_num_value_bits(const QF *qf) {
  return qf->metadata->value_bits;
}
uint64_t qf_get_num_key_remainder_bits(const QF *qf) {
  return qf->metadata->key_remainder_bits;
}
uint64_t qf_get_bits_per_slot(const QF *qf) {
  return qf->metadata->bits_per_slot;
}

void qf_sync_counters(const QF *qf) {
  pc_sync(&qf->runtimedata->pc_nelts);
  pc_sync(&qf->runtimedata->pc_noccupied_slots);
  pc_sync(&qf->runtimedata->pc_rebuild_cd);
}

/* initialize the iterator at the run corresponding
 * to the position index
 */
int64_t qf_iterator_from_position(const QF *qf, QFi *qfi, uint64_t position) {
  if (position == 0xffffffffffffffff) {
    qfi->current = 0xffffffffffffffff;
    qfi->qf = qf;
    return QFI_INVALID;
  }
  assert(position < qf->metadata->nslots);
  if (!is_occupied(qf, position)) {
    uint64_t block_index = position;
    uint64_t idx = bitselect(get_block(qf, block_index)->occupieds[0], 0);
    if (idx == 64) {
      while (idx == 64 && block_index < qf->metadata->nblocks) {
        block_index++;
        idx = bitselect(get_block(qf, block_index)->occupieds[0], 0);
      }
    }
    position = block_index * QF_SLOTS_PER_BLOCK + idx;
  }

  qfi->qf = qf;
  qfi->num_clusters = 0;
  qfi->run = position;
  qfi->current = position == 0 ? 0 : run_end(qfi->qf, position - 1) + 1;
  if (qfi->current < position)
    qfi->current = position;

#ifdef LOG_CLUSTER_LENGTH
  qfi->c_info =
      (cluster_data *)calloc(qf->metadata->nslots / 32, sizeof(cluster_data));
  if (qfi->c_info == NULL) {
    perror("Couldn't allocate memory for c_info.");
    exit(EXIT_FAILURE);
  }
  qfi->cur_start_index = position;
  qfi->cur_length = 1;
#endif

  if (qfi->current >= qf->metadata->nslots)
    return QFI_INVALID;
  return qfi->current;
}

int64_t qf_iterator_from_key_value(const QF *qf, QFi *qfi, uint64_t key,
                                   uint64_t value, uint8_t flags) {
  if (key >= qf->metadata->range) {
    qfi->current = 0xffffffffffffffff;
    qfi->qf = qf;
    return QFI_INVALID;
  }

  qfi->qf = qf;
  qfi->num_clusters = 0;

  if (GET_KEY_HASH(flags) == QF_HASH_INVERTIBLE)
    key = hash_64(key, BITMASK(qf->metadata->key_bits));

  uint64_t hash = (key << qf->metadata->value_bits) |
                  (value & BITMASK(qf->metadata->value_bits));

  uint64_t hash_remainder = hash & BITMASK(qf->metadata->bits_per_slot);
  uint64_t hash_bucket_index = hash >> qf->metadata->bits_per_slot;
  bool flag = false;

  // If a run starts at "position" move the iterator to point it to the
  // smallest key greater than or equal to "hash".
  if (is_occupied(qf, hash_bucket_index)) {
    uint64_t runstart_index =
        hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
    if (runstart_index < hash_bucket_index)
      runstart_index = hash_bucket_index;
    uint64_t current_remainder, current_end;
    do {
      current_end = runstart_index;
      current_remainder = get_slot(qf, current_end);
      if (current_remainder >= hash_remainder) {
        flag = true;
        break;
      }
      runstart_index = current_end + 1;
    } while (!is_runend(qf, current_end));
    // found "hash" or smallest key greater than "hash" in this run.
    if (flag) {
      qfi->run = hash_bucket_index;
      qfi->current = runstart_index;
    }
  }
  // If a run doesn't start at "position" or the largest key in the run
  // starting at "position" is smaller than "hash" then find the start of the
  // next run.
  if (!is_occupied(qf, hash_bucket_index) || !flag) {
    uint64_t position = hash_bucket_index;
    assert(position < qf->metadata->nslots);
    uint64_t block_index = position / QF_SLOTS_PER_BLOCK;
    uint64_t idx = bitselect(get_block(qf, block_index)->occupieds[0], 0);
    if (idx == 64) {
      while (idx == 64 && block_index < qf->metadata->nblocks) {
        block_index++;
        idx = bitselect(get_block(qf, block_index)->occupieds[0], 0);
      }
    }
    position = block_index * QF_SLOTS_PER_BLOCK + idx;
    qfi->run = position;
    qfi->current = position == 0 ? 0 : run_end(qfi->qf, position - 1) + 1;
    if (qfi->current < position)
      qfi->current = position;
  }

  if (qfi->current >= qf->metadata->nslots)
    return QFI_INVALID;
  return qfi->current;
}

static int qfi_get(const QFi *qfi, uint64_t *key, uint64_t *value) {
  if (qfi_end(qfi))
    return QFI_INVALID;

  uint64_t current_remainder = get_slot(qfi->qf, qfi->current);

  *value = current_remainder & BITMASK(qfi->qf->metadata->value_bits);
  current_remainder = current_remainder >> qfi->qf->metadata->value_bits;
  *key =
      (qfi->run << qfi->qf->metadata->key_remainder_bits) | current_remainder;

  return 0;
}

int qfi_get_key(const QFi *qfi, uint64_t *key, uint64_t *value) {
  *key = *value = 0;
  int ret = qfi_get(qfi, key, value);
  if (ret == 0) {
    if (qfi->qf->metadata->hash_mode == QF_HASH_INVERTIBLE)
      *key = hash_64i(*key, BITMASK(qfi->qf->metadata->key_bits));
  }

  return ret;
}

int qfi_get_hash(const QFi *qfi, uint64_t *key, uint64_t *value) {
  *key = *value = 0;
  return qfi_get(qfi, key, value);
}

int qfi_next(QFi *qfi) {
  if (qfi_end(qfi))
    return QFI_INVALID;
  else {
    if (!is_runend(qfi->qf, qfi->current)) {
      qfi->current++;
#ifdef LOG_CLUSTER_LENGTH
      qfi->cur_length++;
#endif
      if (qfi_end(qfi))
        return QFI_INVALID;
      return 0;
    } else {
#ifdef LOG_CLUSTER_LENGTH
      /* save to check if the new current is the new cluster. */
      uint64_t old_current = qfi->current;
#endif
      uint64_t block_index = qfi->run / QF_SLOTS_PER_BLOCK;
      uint64_t rank = bitrank(get_block(qfi->qf, block_index)->occupieds[0],
                              qfi->run % QF_SLOTS_PER_BLOCK);
      uint64_t next_run =
          bitselect(get_block(qfi->qf, block_index)->occupieds[0], rank);
      if (next_run == 64) {
        rank = 0;
        while (next_run == 64 && block_index < qfi->qf->metadata->nblocks) {
          block_index++;
          next_run =
              bitselect(get_block(qfi->qf, block_index)->occupieds[0], rank);
        }
      }
      if (block_index == qfi->qf->metadata->nblocks) {
        /* set the index values to max. */
        qfi->run = qfi->current = qfi->qf->metadata->xnslots;
        return QFI_INVALID;
      }
      qfi->run = block_index * QF_SLOTS_PER_BLOCK + next_run;
      qfi->current++;
      if (qfi->current < qfi->run)
        qfi->current = qfi->run;
#ifdef LOG_CLUSTER_LENGTH
      if (qfi->current > old_current + 1) { /* new cluster. */
        if (qfi->cur_length > 10) {
          qfi->c_info[qfi->num_clusters].start_index = qfi->cur_start_index;
          qfi->c_info[qfi->num_clusters].length = qfi->cur_length;
          qfi->num_clusters++;
        }
        qfi->cur_start_index = qfi->run;
        qfi->cur_length = 1;
      } else {
        qfi->cur_length++;
      }
#endif
      return 0;
    }
  }
}

bool qfi_end(const QFi *qfi) {
  if (qfi->current >=
      qfi->qf->metadata->xnslots /*&& is_runend(qfi->qf, qfi->current)*/)
    return true;
  return false;
}
