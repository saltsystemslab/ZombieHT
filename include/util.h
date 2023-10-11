#ifndef UTIL_H
#define UTIL_H

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "hashutil.h"

/******************************************************************
 * Code for managing the metadata bits and slots w/o interpreting *
 * the content of the slots.
 ******************************************************************/

#define MIN(X, Y) (((X) < (Y)) ? (X) : (Y))
#define MAX(X, Y) (((X) > (Y)) ? (X) : (Y))
#define MAX_VALUE(nbits) ((1ULL << (nbits)) - 1)
#define BITMASK(nbits) ((nbits) == 64 ? 0xffffffffffffffff : MAX_VALUE(nbits))
#define NUM_SLOTS_TO_LOCK (1ULL << 16)
#define CLUSTER_SIZE (1ULL << 14)
#define METADATA_WORD(qf, field, slot_index)                                   \
  (get_block((qf), (slot_index) / QF_SLOTS_PER_BLOCK)                          \
       ->field[((slot_index) % QF_SLOTS_PER_BLOCK) / 64])
#define SET_O(qf, index)                                                       \
  (METADATA_WORD((qf), occupieds, (index)) |=                                  \
   1ULL << ((index) % QF_SLOTS_PER_BLOCK))
#define SET_R(qf, index)                                                       \
  (METADATA_WORD((qf), runends, (index)) |= 1ULL                               \
                                            << ((index) % QF_SLOTS_PER_BLOCK))
#define SET_T(qf, index)                                                       \
  (METADATA_WORD((qf), tombstones, (index)) |=                                 \
   1ULL << ((index) % QF_SLOTS_PER_BLOCK))
#define RESET_O(qf, index)                                                     \
  (METADATA_WORD((qf), occupieds, (index)) &=                                  \
   ~(1ULL << ((index) % QF_SLOTS_PER_BLOCK)))
#define RESET_R(qf, index)                                                     \
  (METADATA_WORD((qf), runends, (index)) &=                                    \
   ~(1ULL << ((index) % QF_SLOTS_PER_BLOCK)))
#define RESET_T(qf, index)                                                     \
  (METADATA_WORD((qf), tombstones, (index)) &=                                 \
   ~(1ULL << ((index) % QF_SLOTS_PER_BLOCK)))
#define GET_NO_LOCK(flag) (flag & QF_NO_LOCK)
#define GET_TRY_ONCE_LOCK(flag) (flag & QF_TRY_ONCE_LOCK)
#define GET_WAIT_FOR_LOCK(flag) (flag & QF_WAIT_FOR_LOCK)
#define GET_KEY_HASH(flag) (flag & QF_KEY_IS_HASH)

#define DISTANCE_FROM_HOME_SLOT_CUTOFF 1000
#define BILLION 1000000000L

#ifdef DEBUG
#define PRINT_DEBUG 1
#else
#define PRINT_DEBUG 0
#endif

#define DEBUG_CQF(fmt, ...)                                                    \
  do {                                                                         \
    if (PRINT_DEBUG)                                                           \
      fprintf(stderr, fmt, __VA_ARGS__);                                       \
  } while (0)
#define DEBUG_CQF(fmt, ...)                                                    \
  do {                                                                         \
    if (PRINT_DEBUG)                                                           \
      fprintf(stderr, fmt, __VA_ARGS__);                                       \
  } while (0)

#define DEBUG_DUMP(qf)                                                         \
  do {                                                                         \
    if (PRINT_DEBUG)                                                           \
      qf_dump_metadata(qf);                                                    \
  } while (0)
#define DEBUG_DUMP(qf)                                                         \
  do {                                                                         \
    if (PRINT_DEBUG)                                                           \
      qf_dump_metadata(qf);                                                    \
  } while (0)

static __inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}


static inline int popcnt(uint64_t val) {
  asm("popcnt %[val], %[val]" : [val] "+r"(val) : : "cc");
  return val;
}

static inline int64_t bitscanreverse(uint64_t val) {
  if (val == 0) {
    return -1;
  } else {
    asm("bsr %[val], %[val]" : [val] "+r"(val) : : "cc");
    return val;
  }
}

static inline int popcntv(const uint64_t val, int ignore) {
  if (ignore % 64)
    return popcnt(val & ~BITMASK(ignore % 64));
  else
    return popcnt(val);
}

// Returns the number of 1s up to (and including) the pos'th bit
// Bits are numbered from 0
static inline int bitrank(uint64_t val, int pos) {
  val = val & ((2ULL << pos) - 1);
  asm("popcnt %[val], %[val]" : [val] "+r"(val) : : "cc");
  return val;
}

/**
 * Returns the position of the k-th 1 in the 64-bit word x.
 * k is 0-based, so k=0 returns the position of the first 1.
 *
 * Uses the broadword selection algorithm by Vigna [1], improved by Gog
 * and Petri [2] and Vigna [3].
 *
 * [1] Sebastiano Vigna. Broadword Implementation of Rank/Select
 *    Queries. WEA, 2008
 *
 * [2] Simon Gog, Matthias Petri. Optimized succinct data
 * structures for massive data. Softw. Pract. Exper., 2014
 *
 * [3] Sebastiano Vigna. MG4J 5.2.1. http://mg4j.di.unimi.it/
 * The following code is taken from
 * https://github.com/facebook/folly/blob/b28186247104f8b90cfbe094d289c91f9e413317/folly/experimental/Select64.h
 */
const uint8_t kSelectInByte[2048] = {
    8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3,
    0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0,
    1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1,
    0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0,
    2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2,
    0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0,
    1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1,
    0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0,
    3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5,
    0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0,
    1, 0, 2, 0, 1, 0, 8, 8, 8, 1, 8, 2, 2, 1, 8, 3, 3, 1, 3, 2, 2, 1, 8, 4, 4,
    1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1,
    3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 6, 6, 1, 6,
    2, 2, 1, 6, 3, 3, 1, 3, 2, 2, 1, 6, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2,
    2, 1, 6, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2,
    1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 7, 7, 1, 7, 2, 2, 1, 7, 3, 3, 1, 3, 2, 2, 1,
    7, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 7, 5, 5, 1, 5, 2, 2, 1, 5,
    3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 7, 6,
    6, 1, 6, 2, 2, 1, 6, 3, 3, 1, 3, 2, 2, 1, 6, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3,
    1, 3, 2, 2, 1, 6, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1,
    4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 8, 8, 8, 8, 8, 8, 2, 8, 8, 8, 3, 8,
    3, 3, 2, 8, 8, 8, 4, 8, 4, 4, 2, 8, 4, 4, 3, 4, 3, 3, 2, 8, 8, 8, 5, 8, 5,
    5, 2, 8, 5, 5, 3, 5, 3, 3, 2, 8, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3,
    2, 8, 8, 8, 6, 8, 6, 6, 2, 8, 6, 6, 3, 6, 3, 3, 2, 8, 6, 6, 4, 6, 4, 4, 2,
    6, 4, 4, 3, 4, 3, 3, 2, 8, 6, 6, 5, 6, 5, 5, 2, 6, 5, 5, 3, 5, 3, 3, 2, 6,
    5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3, 2, 8, 8, 8, 7, 8, 7, 7, 2, 8, 7,
    7, 3, 7, 3, 3, 2, 8, 7, 7, 4, 7, 4, 4, 2, 7, 4, 4, 3, 4, 3, 3, 2, 8, 7, 7,
    5, 7, 5, 5, 2, 7, 5, 5, 3, 5, 3, 3, 2, 7, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3,
    4, 3, 3, 2, 8, 7, 7, 6, 7, 6, 6, 2, 7, 6, 6, 3, 6, 3, 3, 2, 7, 6, 6, 4, 6,
    4, 4, 2, 6, 4, 4, 3, 4, 3, 3, 2, 7, 6, 6, 5, 6, 5, 5, 2, 6, 5, 5, 3, 5, 3,
    3, 2, 6, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3, 2, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 3, 8, 8, 8, 8, 8, 8, 8, 4, 8, 8, 8, 4, 8, 4, 4, 3,
    8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 3, 8, 8, 8, 5, 8, 5, 5, 4, 8,
    5, 5, 4, 5, 4, 4, 3, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 3, 8, 8,
    8, 6, 8, 6, 6, 4, 8, 6, 6, 4, 6, 4, 4, 3, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6,
    5, 6, 5, 5, 3, 8, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 8, 8, 8, 8,
    8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 3, 8, 8, 8, 7, 8, 7, 7, 4, 8, 7, 7, 4, 7,
    4, 4, 3, 8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 3, 8, 7, 7, 5, 7, 5,
    5, 4, 7, 5, 5, 4, 5, 4, 4, 3, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6,
    3, 8, 7, 7, 6, 7, 6, 6, 4, 7, 6, 6, 4, 6, 4, 4, 3, 8, 7, 7, 6, 7, 6, 6, 5,
    7, 6, 6, 5, 6, 5, 5, 3, 7, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8,
    8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 4, 8, 8, 8, 8, 8,
    8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 5, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6, 5, 6, 5,
    5, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8,
    7, 8, 8, 8, 7, 8, 7, 7, 4, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5,
    8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 4, 8, 8, 8, 8, 8, 8, 8, 7, 8,
    8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 4, 8, 8,
    8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5, 8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6,
    5, 6, 5, 5, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6,
    8, 8, 8, 6, 8, 6, 6, 5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7,
    8, 7, 7, 6, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8,
    7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7};

static inline uint64_t _select64(uint64_t x, int k) {
  if (k >= popcnt(x)) {
    return 64;
  }

  const uint64_t kOnesStep4 = 0x1111111111111111ULL;
  const uint64_t kOnesStep8 = 0x0101010101010101ULL;
  const uint64_t kMSBsStep8 = 0x80ULL * kOnesStep8;

  uint64_t s = x;
  s = s - ((s & 0xA * kOnesStep4) >> 1);
  s = (s & 0x3 * kOnesStep4) + ((s >> 2) & 0x3 * kOnesStep4);
  s = (s + (s >> 4)) & 0xF * kOnesStep8;
  uint64_t byteSums = s * kOnesStep8;

  uint64_t kStep8 = k * kOnesStep8;
  uint64_t geqKStep8 = (((kStep8 | kMSBsStep8) - byteSums) & kMSBsStep8);
  uint64_t place = popcnt(geqKStep8) * 8;
  uint64_t byteRank = k - (((byteSums << 8) >> place) & (uint64_t)(0xFF));
  return place + kSelectInByte[((x >> place) & 0xFF) | (byteRank << 8)];
}

/* Returns the position of the rank'th 1.  (rank = 0 returns the 1st 1)
 * Rank %= 64.
 * Returns 64 if there are fewer than rank+1 1s.
 * Little-endian code, rank from right to left.
 */
static inline uint64_t bitselect(uint64_t val, int rank) {
#ifdef __SSE4_2_
  uint64_t i = 1ULL << rank;
  asm("pdep %[val], %[mask], %[val]" : [val] "+r"(val) : [mask] "r"(i));
  asm("tzcnt %[bit], %[index]" : [index] "=r"(i) : [bit] "g"(val) : "cc");
  return i;
#endif
  return _select64(val, rank);
}

// Returns the position of the rank'th 1 from right, ignoring the first
// `ignore` bits.
static inline uint64_t bitselectv(const uint64_t val, int ignore, int rank) {
  if (rank >= 64) return 64;
  return bitselect(val & ~BITMASK(ignore % 64), rank);
}

static inline uint64_t bitselect_big(uint64_t val) {
  uint64_t i;
  asm("lzcnt %[bit], %[index]" : [index] "=r"(i) : [bit] "g"(val) : "cc");
  return i;
}

static inline int is_runend(const QF *qf, uint64_t index) {
  return (METADATA_WORD(qf, runends, index) >>
          ((index % QF_SLOTS_PER_BLOCK) % 64)) &
         1ULL;
}

static inline int is_occupied(const QF *qf, uint64_t index) {
  return (METADATA_WORD(qf, occupieds, index) >>
          ((index % QF_SLOTS_PER_BLOCK) % 64)) &
         1ULL;
}


/* Return num of runends in range [start, start+end). */
static inline size_t runends_cnt(const QF *qf, size_t start, size_t len) {
  size_t cnt = 0;
  size_t end = start + len;
  size_t block_i = start / QF_SLOTS_PER_BLOCK;
  size_t bstart = start % QF_SLOTS_PER_BLOCK;
  do {
    size_t word = get_block(qf, block_i)->runends[0];
    cnt += popcntv(word, bstart);
    block_i++;
    bstart = 0;
  } while ((block_i) * QF_SLOTS_PER_BLOCK <= end);
  size_t word = get_block(qf, block_i-1)->runends[0];
  cnt -= popcntv(word, end % QF_SLOTS_PER_BLOCK);
  return cnt;
}

/* Return num of occupieds in range [start, start+end). */
static inline size_t occupieds_cnt(const QF *qf, size_t start, size_t len) {
  size_t cnt = 0;
  size_t end = start + len;
  size_t block_i = start / QF_SLOTS_PER_BLOCK;
  size_t bstart = start % QF_SLOTS_PER_BLOCK;
  do {
    size_t word = get_block(qf, block_i)->occupieds[0];
    cnt += popcntv(word, bstart);
    block_i++;
    bstart = 0;
  } while ((block_i) * QF_SLOTS_PER_BLOCK <= end);
  size_t word = get_block(qf, block_i-1)->occupieds[0];
  cnt -= popcntv(word, end % QF_SLOTS_PER_BLOCK);
  return cnt;
}

/* Return pos of the next r occupieds in range [index, nslots). 0 for the first.
 * If not found, return nslots.
 */
static inline size_t occupieds_select(const QF *qf, size_t index, size_t r) {
  size_t block_i = index / QF_SLOTS_PER_BLOCK;
  size_t bstart = index % QF_SLOTS_PER_BLOCK;
  do {
    size_t word = get_block(qf, block_i)->occupieds[0];
    size_t pos = bitselectv(word, bstart, r);
    if (pos < sizeof(word) * 8)
      return block_i * QF_SLOTS_PER_BLOCK + pos;
    r -= popcntv(word, bstart);
    bstart = 0;
    block_i++;
  } while (block_i < qf->metadata->nblocks);
  return qf->metadata->nslots;
}

/* Return pos of the next r runends in range [index, nslots). 0 for the first.
 * If not found, return nslots.
 */
static inline size_t runends_select(const QF *qf, size_t index, size_t r) {
  size_t block_i = index / QF_SLOTS_PER_BLOCK;
  size_t bstart = index % QF_SLOTS_PER_BLOCK;
  do {
    uint64_t word = get_block(qf, block_i)->runends[0];
    size_t pos = bitselectv(word, bstart, r);
    if (pos < sizeof(word) * 8)
      return block_i * QF_SLOTS_PER_BLOCK + pos;
    r -= popcntv(word, bstart);
    bstart = 0;
    block_i++;
  } while (block_i < qf->metadata->nblocks);
  fprintf(stderr, "runends_select: reached xnslots.\n");
  return qf->metadata->xnslots;
}

#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 ||                         \
    QF_BITS_PER_SLOT == 32 || QF_BITS_PER_SLOT == 64

static inline uint64_t get_slot(const QF *qf, uint64_t index) {
#ifdef DEBUG
  assert(index < qf->metadata->xnslots);
#endif
  return get_block(qf, index / QF_SLOTS_PER_BLOCK)
      ->slots[index % QF_SLOTS_PER_BLOCK];
}

static inline void set_slot(const QF *qf, uint64_t index, uint64_t value) {
#ifdef DEBUG
  assert(index < qf->metadata->xnslots);
#endif
  get_block(qf, index / QF_SLOTS_PER_BLOCK)->slots[index % QF_SLOTS_PER_BLOCK] =
      value & BITMASK(qf->metadata->bits_per_slot);
}

#elif QF_BITS_PER_SLOT > 0

/* Little-endian code ....  Big-endian is TODO */

static inline uint64_t get_slot(const QF *qf, uint64_t index) {
  /* Should use __uint128_t to support up to 64-bit remainders, but gcc seems
   * to generate buggy code.  :/  */
  // assert(index < qf->metadata->xnslots);
  uint64_t *p =
      (uint64_t *)&get_block(qf, index / QF_SLOTS_PER_BLOCK)
          ->slots[(index % QF_SLOTS_PER_BLOCK) * QF_BITS_PER_SLOT / 8];
  return (uint64_t)(((*p) >>
                     (((index % QF_SLOTS_PER_BLOCK) * QF_BITS_PER_SLOT) % 8)) &
                    BITMASK(QF_BITS_PER_SLOT));
}

static inline void set_slot(const QF *qf, uint64_t index, uint64_t value) {
  /* Should use __uint128_t to support up to 64-bit remainders, but gcc seems
   * to generate buggy code.  :/  */
  // assert(index < qf->metadata->xnslots);
  uint64_t *p =
      (uint64_t *)&get_block(qf, index / QF_SLOTS_PER_BLOCK)
          ->slots[(index % QF_SLOTS_PER_BLOCK) * QF_BITS_PER_SLOT / 8];
  uint64_t t = *p;
  uint64_t mask = BITMASK(QF_BITS_PER_SLOT);
  uint64_t v = value;
  int shift = ((index % QF_SLOTS_PER_BLOCK) * QF_BITS_PER_SLOT) % 8;
  mask <<= shift;
  v <<= shift;
  t &= ~mask;
  t |= v;
  *p = t;
}

#else

/* Little-endian code ....  Big-endian is TODO */

static inline uint64_t get_slot(const QF *qf, uint64_t index) {
  // assert(index < qf->metadata->xnslots);
  /* Should use __uint128_t to support up to 64-bit remainders, but gcc seems
   * to generate buggy code.  :/  */
  uint64_t *p = (uint64_t *)&get_block(qf, index / QF_SLOTS_PER_BLOCK)
                    ->slots[(index % QF_SLOTS_PER_BLOCK) *
                            qf->metadata->bits_per_slot / 8];
  return (uint64_t)(((*p) >> (((index % QF_SLOTS_PER_BLOCK) *
                               qf->metadata->bits_per_slot) %
                              8)) &
                    BITMASK(qf->metadata->bits_per_slot));
}

static inline void set_slot(const QF *qf, uint64_t index, uint64_t value) {
  // assert(index < qf->metadata->xnslots);
  /* Should use __uint128_t to support up to 64-bit remainders, but gcc seems
   * to generate buggy code.  :/  */
  uint64_t *p = (uint64_t *)&get_block(qf, index / QF_SLOTS_PER_BLOCK)
                    ->slots[(index % QF_SLOTS_PER_BLOCK) *
                            qf->metadata->bits_per_slot / 8];
  uint64_t t = *p;
  uint64_t mask = BITMASK(qf->metadata->bits_per_slot);
  uint64_t v = value;
  int shift = ((index % QF_SLOTS_PER_BLOCK) * qf->metadata->bits_per_slot) % 8;
  mask <<= shift;
  v <<= shift;
  t &= ~mask;
  t |= v;
  *p = t;
}

#endif

static inline uint64_t get_slot_remainder(const QF *qf, uint64_t index) {
  return get_slot(qf, index )>> (qf->metadata->value_bits);
}

static inline int offset_lower_bound(const QF *qf, uint64_t slot_index);
static inline uint64_t run_end(const QF *qf, uint64_t hash_bucket_index);
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
static void _recalculate_block_offsets(QF *qf, size_t from_index, size_t to_index);
#else
static void _recalculate_block_offsets(QF *qf, size_t run);
#endif


static inline uint64_t block_offset(const QF *qf, uint64_t blockidx) {
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
  return get_block(qf, blockidx)->offset;
#else
  if (blockidx == 0)
    return 0;
  /* If we have extended counters and a 16-bit (or larger) offset
           field, then we can safely ignore the possibility of overflowing
           that field. */
  if (sizeof(qf->blocks[0].offset) > 1 ||
      get_block(qf, blockidx)->offset <
          BITMASK(8 * sizeof(qf->blocks[0].offset)))
    return get_block(qf, blockidx)->offset;
  else abort();

  return run_end(qf, QF_SLOTS_PER_BLOCK * blockidx - 1) -
         QF_SLOTS_PER_BLOCK * blockidx + 1;
#endif
}

/* Assume the block offset of `hash_bucket_index` is correct,
 * Find the biggest run which is less or equal to `hash_bucket_index`,
 * Then find the end of the run. 
 * Return max(end, hash_bucket_index) */
static inline uint64_t run_end(const QF *qf, uint64_t hash_bucket_index) {
  uint64_t bucket_block_index = hash_bucket_index / QF_SLOTS_PER_BLOCK;
  uint64_t bucket_intrablock_offset = hash_bucket_index % QF_SLOTS_PER_BLOCK;
  uint64_t bucket_blocks_offset = block_offset(qf, bucket_block_index);
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
  size_t diff = offset_lower_bound(qf, hash_bucket_index);
  if (diff == 0) return hash_bucket_index;  // empty slot.
  return runends_select(qf, hash_bucket_index, diff-1);
#else
  uint64_t bucket_intrablock_rank =
      bitrank(get_block(qf, bucket_block_index)->occupieds[0],
              bucket_intrablock_offset);

  if (bucket_intrablock_rank == 0) {
    if (bucket_blocks_offset <= bucket_intrablock_offset)
      return hash_bucket_index;
    else
      return QF_SLOTS_PER_BLOCK * bucket_block_index + bucket_blocks_offset - 1;
  }

  uint64_t runend_block_index =
      bucket_block_index + bucket_blocks_offset / QF_SLOTS_PER_BLOCK;
  uint64_t runend_ignore_bits = bucket_blocks_offset % QF_SLOTS_PER_BLOCK;
  uint64_t runend_rank = bucket_intrablock_rank - 1;
  uint64_t runend_block_offset =
      bitselectv(get_block(qf, runend_block_index)->runends[0],
                 runend_ignore_bits, runend_rank);
  while (runend_block_offset == QF_SLOTS_PER_BLOCK) {
    runend_rank -= popcntv(get_block(qf, runend_block_index)->runends[0],
                            runend_ignore_bits);
    runend_block_index++;
    runend_ignore_bits = 0;
    runend_block_offset =
        bitselectv(get_block(qf, runend_block_index)->runends[0],
                    runend_ignore_bits, runend_rank);
  }

  uint64_t runend_index =
      QF_SLOTS_PER_BLOCK * runend_block_index + runend_block_offset;
  if (runend_index < hash_bucket_index)
    return hash_bucket_index;
  else
    return runend_index;
#endif
}

/* Return n_occupieds in [0, slot_index] minus n_runends in [0, slot_index) */
static inline int offset_lower_bound(const QF *qf, uint64_t slot_index) {
  const size_t block_id = slot_index / QF_SLOTS_PER_BLOCK;
  const qfblock *b = get_block(qf, block_id);
  const uint64_t slot_offset = slot_index % QF_SLOTS_PER_BLOCK;
  const uint64_t boffset = block_offset(qf, block_id);
  const uint64_t occupieds = b->occupieds[0] & BITMASK(slot_offset + 1);
  const uint64_t runends = b->runends[0] & BITMASK(slot_offset);
  assert(QF_SLOTS_PER_BLOCK == 64);
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
  return popcnt(occupieds) + boffset - popcnt(runends);
#else
  if (boffset <= slot_offset) {
    return popcnt(occupieds) - popcnt(runends >> boffset);
  }
  return boffset - slot_offset + popcnt(occupieds);
#endif
}

static inline int is_empty(const QF *qf, uint64_t slot_index) {
  return offset_lower_bound(qf, slot_index) == 0;
}

static inline int find_first_empty_slot(QF *qf, uint64_t from, uint64_t *empty_slot) {
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
  size_t block_i = from / QF_SLOTS_PER_BLOCK;
  size_t bstart = from % QF_SLOTS_PER_BLOCK;
  int diff = offset_lower_bound(qf, from);  // diff between occupieds and runends
  while (diff > 0)
  {
    size_t next = runends_select(qf, from, diff - 1) + 1;
    if (next == qf->metadata->xnslots)
      return QF_NO_SPACE;
    diff = occupieds_cnt(qf, from + 1, next - from);
    from = next;
  }
  *empty_slot = from;
  return 0;
#else
  do {
    int t = offset_lower_bound(qf, from);
    if (t < 0) {
      return -1;
    }
    if (t == 0)
      break;
    from = from + t;
  } while (1);
  *empty_slot = from;
  return 0;
#endif
}

/* Return a new word, which first copy `b`, then shift the part (bend, bstart]
 * to the left by `amount`, keep anything out side of the range unchanged.
 * big endian, index from right to left. 
 * if bstart == 0, copy the right `amout` bits from `a` into the right side of
 * the new word. 
 * returns the new word.
 */
static inline uint64_t shift_into_b(const uint64_t a, const uint64_t b,
                                    const int bstart, const int bend,
                                    const int amount) {
  const uint64_t a_component = bstart == 0 ? (a >> (64 - amount)) : 0;
  const uint64_t b_shifted_mask = BITMASK(bend - bstart) << bstart;
  const uint64_t b_shifted = ((b_shifted_mask & b) << amount) & b_shifted_mask;
  const uint64_t b_mask = ~b_shifted_mask;
  return (a_component&b_shifted_mask) | b_shifted | (b & b_mask);
}

#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 ||                         \
    QF_BITS_PER_SLOT == 32 || QF_BITS_PER_SLOT == 64

static inline void shift_remainders(QF *qf, uint64_t start_index,
                                    uint64_t empty_index) {
  uint64_t start_block = start_index / QF_SLOTS_PER_BLOCK;
  uint64_t start_offset = start_index % QF_SLOTS_PER_BLOCK;
  uint64_t empty_block = empty_index / QF_SLOTS_PER_BLOCK;
  uint64_t empty_offset = empty_index % QF_SLOTS_PER_BLOCK;
#ifdef DEBUG
  assert(start_index <= empty_index && empty_index < qf->metadata->xnslots);
#endif
  while (start_block < empty_block) {
    memmove(&get_block(qf, empty_block)->slots[1],
            &get_block(qf, empty_block)->slots[0],
            empty_offset * sizeof(qf->blocks[0].slots[0]));
    get_block(qf, empty_block)->slots[0] =
        get_block(qf, empty_block - 1)->slots[QF_SLOTS_PER_BLOCK - 1];
    empty_block--;
    empty_offset = QF_SLOTS_PER_BLOCK - 1;
  }

  memmove(&get_block(qf, empty_block)->slots[start_offset + 1],
          &get_block(qf, empty_block)->slots[start_offset],
          (empty_offset - start_offset) * sizeof(qf->blocks[0].slots[0]));
}

// Shift [start_index, end_index] by dist slots to the left.
static inline void shift_remainders_left(QF *qf, uint64_t start_index,
                                    uint64_t end_index, uint64_t dist) {
    uint64_t total_slots_to_shift = (end_index - start_index + 1);
    uint64_t slots_shifted = 0;
    size_t dst_index = start_index - dist;
    while (slots_shifted < total_slots_to_shift) {
      size_t start_block = start_index / QF_SLOTS_PER_BLOCK;
      size_t start_block_offset = start_index % QF_SLOTS_PER_BLOCK;
      size_t dst_block = dst_index / QF_SLOTS_PER_BLOCK;
      size_t dst_block_offset = dst_index % QF_SLOTS_PER_BLOCK;

      size_t src_window_size = MIN(QF_SLOTS_PER_BLOCK - start_block_offset, end_index - start_index+1);
      size_t dst_window_size = (QF_SLOTS_PER_BLOCK - dst_block_offset);
      
      size_t slots_to_shift = MIN(src_window_size, dst_window_size);
      memmove(
        &get_block(qf, dst_block)->slots[dst_block_offset],
        &get_block(qf, start_block)->slots[start_block_offset], 
        slots_to_shift * sizeof(qf->blocks[0].slots[0]));
      #if 0
      // To debug this, compare with the version that moves slot bt slot.
      printf("Block Moving [%ld, %ld] to [%ld %ld]\n", 
        start_index, 
        start_index + slots_to_shift-1, 
        dst_index, 
        dst_index + slots_to_shift-1);
      #endif
      start_index += slots_to_shift;
      dst_index += slots_to_shift;
      slots_shifted += slots_to_shift;
    }
}

#else

#define REMAINDER_WORD(qf, i)                                                  \
  ((uint64_t *)&(get_block(qf, (i) / qf->metadata->bits_per_slot)              \
                     ->slots[8 * ((i) % qf->metadata->bits_per_slot)]))

/* shift slots in range [start_index, empty_index) by 1 to the big end. 
 * slot empty_index will be replaced by slot empty_index-1
 */
static inline void shift_remainders(QF *qf, const uint64_t start_index,
                                    const uint64_t empty_index) {
  uint64_t last_word = ((empty_index + 1) * qf->metadata->bits_per_slot - 1) / 64;
  const uint64_t first_word = start_index * qf->metadata->bits_per_slot / 64;
  int bend = ((empty_index + 1) * qf->metadata->bits_per_slot - 1) % 64 + 1;
  const int bstart = (start_index * qf->metadata->bits_per_slot) % 64;

  while (last_word != first_word) {
    *REMAINDER_WORD(qf, last_word) = shift_into_b(
        *REMAINDER_WORD(qf, last_word - 1), *REMAINDER_WORD(qf, last_word), 0,
        bend, qf->metadata->bits_per_slot);
    last_word--;
    bend = 64;
  }
  *REMAINDER_WORD(qf, last_word) =
      shift_into_b(0, *REMAINDER_WORD(qf, last_word), bstart, bend,
                   qf->metadata->bits_per_slot);
}

static inline void shift_remainders_left(QF *qf, uint64_t start_index,
                                    uint64_t end_index, int dist) {
    // TODO: Make this a memmove.
    for (size_t cur_idx = start_index; cur_idx <= end_index; cur_idx++) {
      set_slot(qf, cur_idx-dist, get_slot(qf, cur_idx));
    }
}

#endif
static inline void qf_dump_block_long(const QF *qf, uint64_t i) {
  uint64_t j;

#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 || QF_BITS_PER_SLOT == 32
  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf("%02x ", get_block(qf, i)->slots[j]);
#elif QF_BITS_PER_SLOT == 64
  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf("%02lx ", get_block(qf, i)->slots[j]);
#else
  printf("BL O R T V\n");
  for (j = 0; j < QF_SLOTS_PER_BLOCK;  j++) {
    printf("%02lx", j); // , get_block(qf, i)->slots[j]);
    printf(" %d",
           (get_block(qf, i)->occupieds[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
    printf(" %d",
           (get_block(qf, i)->runends[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
#ifdef QF_TOMBSTONE
    printf(" %d ",
           (get_block(qf, i)->tombstones[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
#endif
    uint64_t slot = i * QF_SLOTS_PER_BLOCK + j;
    if (slot < qf->metadata->xnslots) {
      printf("%" PRIx64, get_slot(qf, i*QF_SLOTS_PER_BLOCK + j));
    } else {
      printf("-");
    }
    printf("\n");
  }
#endif
}

static inline void qf_dump_block(const QF *qf, uint64_t i) {
  uint64_t j;

  printf("%-192d", get_block(qf, i)->offset);
  printf("\n");

  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf("%02lx ", j);
  printf("\n");

  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf(" %d ",
           (get_block(qf, i)->occupieds[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
  printf("\n");

  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf(" %d ",
           (get_block(qf, i)->runends[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
  printf("\n");

#ifdef QF_TOMBSTONE
  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf(" %d ",
           (get_block(qf, i)->tombstones[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
  printf("\n");
#endif

#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 || QF_BITS_PER_SLOT == 32
  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf("%02x ", get_block(qf, i)->slots[j]);
#elif QF_BITS_PER_SLOT == 64
  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf("%02lx ", get_block(qf, i)->slots[j]);
#else
  for (j = 0; j < QF_SLOTS_PER_BLOCK * qf->metadata->bits_per_slot / 8; j++)
    printf("%02x ", get_block(qf, i)->slots[j]);
#endif

  printf("\n");

  printf("\n");
}

static inline void shift_slots(QF *qf, int64_t first, uint64_t last,
                               uint64_t distance) {
  int64_t i;
  if (distance == 1)
    shift_remainders(qf, first, last + 1);
  else
    for (i = last; i >= first; i--)
      set_slot(qf, i + distance, get_slot(qf, i));
}

// RHM need this function to shift the runends without tombstones.
static inline void shift_runends(QF *qf, int64_t first, uint64_t last,
                                 uint64_t distance) {
  assert(last < qf->metadata->xnslots && distance < 64);
  uint64_t first_word = first / 64;
  uint64_t bstart = first % 64;
  uint64_t last_word = (last + distance + 1) / 64;
  uint64_t bend = (last + distance + 1) % 64;

  if (last_word != first_word) {
    METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
        METADATA_WORD(qf, runends, 64 * (last_word - 1)),
        METADATA_WORD(qf, runends, 64 * last_word), 0, bend, distance);
    bend = 64;
    last_word--;
    while (last_word != first_word) {
      METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
          METADATA_WORD(qf, runends, 64 * (last_word - 1)),
          METADATA_WORD(qf, runends, 64 * last_word), 0, bend, distance);
      last_word--;
    }
  }
  METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
      0, METADATA_WORD(qf, runends, 64 * last_word), bstart, bend, distance);
}


static inline int
remove_replace_slots_and_shift_remainders_and_runends_and_offsets(
    QF *qf,
    int operation,              // only_item_in_the_run
    uint64_t bucket_index,      // Home slot, or the quotient part of the hash
    uint64_t overwrite_index,   // index of the start of the remainder
    const uint64_t *remainders, // the new counter
    uint64_t total_remainders,  // length of the new counter
    uint64_t old_length) {
  uint64_t i;

  // Update the slots
  for (i = 0; i < total_remainders; i++)
    set_slot(qf, overwrite_index + i, remainders[i]);

  // If this is the last thing in its run, then we may need to set a new runend
  // bit
  if (is_runend(qf, overwrite_index + old_length - 1)) {
    if (total_remainders > 0) {
      // If we're not deleting this entry entirely, then it will still the last
      // entry in this run
      SET_R(qf, overwrite_index + total_remainders - 1);
    } else if (overwrite_index > bucket_index &&
               !is_runend(qf, overwrite_index - 1)) {
      // If we're deleting this entry entirely, but it is not the first entry in
      // this run, then set the preceding entry to be the runend
      SET_R(qf, overwrite_index - 1);
    }
  }

  // shift slots back one run at a time
  uint64_t original_bucket = bucket_index;
  uint64_t current_bucket = bucket_index; // the home slot of run to shift
  uint64_t current_slot =
      overwrite_index + total_remainders; // the slot to shift to
  uint64_t current_distance =
      old_length - total_remainders;      // the distance to shift
  int ret_current_distance = current_distance;

  while (current_distance > 0) {
    // when we reach the end of a run
    if (is_runend(qf, current_slot + current_distance - 1)) {
      // find the next run to shift
      do {
        current_bucket++;
      } while (current_bucket < current_slot + current_distance &&
               !is_occupied(qf, current_bucket));
    }
    // shift one slot by current_distance to the current_slot
    if (current_bucket <= current_slot) {
      set_slot(qf, current_slot, get_slot(qf, current_slot + current_distance));
      if (is_runend(qf, current_slot) !=
          is_runend(qf, current_slot + current_distance))
        METADATA_WORD(qf, runends, current_slot) ^= 1ULL << (current_slot % 64);
      current_slot++;
      // when we reached the end of the cluster
    } else if (current_bucket <= current_slot + current_distance) {
      uint64_t i;
      for (i = current_slot; i < current_slot + current_distance; i++) {
        set_slot(qf, i, 0);
        RESET_R(qf, i);
      }

      current_distance = current_slot + current_distance - current_bucket;
      current_slot = current_bucket;
    } else {
      current_distance = 0;
    }
  }

  // reset the occupied bit of the hash bucket index if the hash is the
  // only item in the run and is removed completely.
  if (operation && !total_remainders)
    RESET_O(qf, bucket_index);

#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
  _recalculate_block_offsets(qf, original_bucket, current_slot);
#else
  _recalculate_block_offsets(qf, original_bucket);
#endif

  int num_slots_freed = old_length - total_remainders;
  qf->metadata->noccupied_slots -= num_slots_freed;

  return ret_current_distance;
}

/*****************************************************************************
 * Code that uses the above to implement a QF with keys and valuess.         *
 *****************************************************************************/

/* return the next slot which corresponds to a
 * different element
 * */
static inline uint64_t next_slot(QF *qf, uint64_t current) {
  uint64_t rem = get_slot(qf, current);
  current++;

  while (get_slot(qf, current) == rem && current <= qf->metadata->nslots) {
    current++;
  }
  return current;
}

/* Return the hash of the key. */
static inline uint64_t key2hash(const QF *qf, const uint64_t key,
                                const uint8_t flags) {
  if (GET_KEY_HASH(flags) == QF_HASH_INVERTIBLE)
    return hash_64(key, BITMASK(qf->metadata->key_bits));
  return key & BITMASK(qf->metadata->key_bits);
}

/* split the hash into quotient and remainder. */
static inline void quotien_remainder(const QF *qf, const uint64_t hash,
                                     uint64_t *const quotient,
                                     uint64_t *const remainder) {
  *quotient = hash >> qf->metadata->key_remainder_bits;
  *remainder = hash & BITMASK(qf->metadata->key_remainder_bits);
}

/* Find the index of the hash (quotient+remainder) and the range of the run.
 * The range will be [run_start_index, run_end_index).
 * Return:
 * 		1: if found it.
 * run_end_index 
 *    0: if didn't find it, the index and range would be where to insert it.
 */

static inline int64_t bitscanforward(uint64_t val)
{
	if (val == 0) {
		return 64;
	} else {
		asm("bsf %[val], %[val]"
				: [val] "+r" (val)
				:
				: "cc");
		return val;
	}
}

// Mask out the right most `ignore` bits, returns the position of the least
// significant set bit (1 bit).
static inline uint64_t bsf_from(const uint64_t val, int from)
{
	return bitscanforward(val & ~BITMASK(from));
}


/* Return the start index of a run if it exists or where it should be. */
static size_t run_start(const QF *const qf, const size_t quotient) {
  if (quotient == 0) return 0;
  else return run_end(qf, quotient - 1) + 1;
}


/* Find next occupied in [run, nslots). Return nslots if no such one. */
static size_t find_next_run(const QF *qf, size_t run) {
  if (is_occupied(qf, run))
    return run;
  size_t block_index = run / QF_SLOTS_PER_BLOCK;
  size_t slot_offset = run % QF_SLOTS_PER_BLOCK;
  slot_offset = bsf_from(get_block(qf, block_index)->occupieds[0], slot_offset);
  while (slot_offset == QF_SLOTS_PER_BLOCK) {
    ++block_index;
    if (block_index * QF_SLOTS_PER_BLOCK >= qf->metadata->nslots)
      return qf->metadata->xnslots;
    slot_offset = bsf_from(get_block(qf, block_index)->occupieds[0], 0);
  }
  return slot_offset + block_index * QF_SLOTS_PER_BLOCK;
}

/* Update the block offsets of the following blocks.
 * Assume the current block offset is correct.
 */
#ifdef _BLOCKOFFSET_4_NUM_RUNENDS
static void _recalculate_block_offsets(QF *qf, size_t from_index, size_t to_index) {
/* If the block offset is the num of overflowed runends.
 * Assume the current block offset, recalculate the following block offsets
 * until found a correct one again.
 */
  size_t from_b = from_index / QF_SLOTS_PER_BLOCK;
  size_t to_b = to_index / QF_SLOTS_PER_BLOCK;
  qfblock *block = get_block(qf, from_b);
  size_t offset = block->offset;
  while (from_b < to_b) {
    // calculate the next block offset
    size_t n_occupieds = popcnt(block->occupieds[0]);
    size_t n_runends = popcnt(block->runends[0]);
    offset = offset + n_occupieds - n_runends;
    // update the next block offset
    block = get_block(qf, ++from_b);
    block->offset = offset;
  }
  assert(from_b < qf->metadata->nblocks);
}
#else
static void _recalculate_block_offsets(QF *qf, size_t index) {
  size_t block_id = index / QF_SLOTS_PER_BLOCK;
  unsigned int next_offset;
  while (block_id < qf->metadata->nblocks) {
    size_t block_last_run =
        QF_SLOTS_PER_BLOCK * block_id + (QF_SLOTS_PER_BLOCK - 1);
    size_t block_runend_index = run_end(qf, block_last_run);
    // runend spans across the block
    // update the offset of the next block
    if (block_runend_index <= block_last_run) {
      // if the run ends in the same block
      next_offset = 0;
    } else { // if the last run spans across the block
      next_offset = block_runend_index - block_last_run;
      // printf("block: %u, offset: %u\n", original_block, block_offset);
      assert(next_offset < 255);
    }
    block_id++;
    if (next_offset >= 255) {
      printf("block: %lu, offset: %u\n", block_id, next_offset);
      get_block(qf, block_id)->offset = 255;
      continue;
    }
    if (block_offset(qf, block_id) == next_offset)
      break;
    get_block(qf, block_id)->offset = next_offset;
  }
}
#endif

// Reset all tombstone bits from [from_index, to_index]
static inline void reset_tombstone_block(QF *qf, size_t from_index, size_t to_index) {
  size_t from_block, from_block_offset, target_index, block_end_index, block_end_offset, mask;
  from_block = from_index / QF_SLOTS_PER_BLOCK;
  block_end_index = MIN((from_block + 1)* QF_SLOTS_PER_BLOCK - 1, to_index);
  while (true) {
    block_end_offset = block_end_index % QF_SLOTS_PER_BLOCK;
    from_block_offset = from_index % QF_SLOTS_PER_BLOCK;
    mask = BITMASK(QF_SLOTS_PER_BLOCK) ^ (BITMASK(block_end_offset+1) ^ BITMASK(from_block_offset));
    METADATA_WORD(qf, tombstones, block_end_index) &= mask;
    from_index = block_end_index + 1;
    if (from_index > to_index) break;
    from_block++;
    block_end_index = MIN((from_block + 1)* QF_SLOTS_PER_BLOCK - 1, to_index);
  }
}

// Set all tombstone bits from [from_index, to_index]
static inline void set_tombstone_block(QF *qf, size_t from_index, size_t to_index) {
  size_t from_block, from_block_offset, target_index, block_end_index, block_end_offset, mask;
  from_block = from_index / QF_SLOTS_PER_BLOCK;
  block_end_index = MIN((from_block + 1)* QF_SLOTS_PER_BLOCK - 1, to_index);
  while (true) {
    block_end_offset = block_end_index % QF_SLOTS_PER_BLOCK;
    from_block_offset = from_index % QF_SLOTS_PER_BLOCK;
    mask = (BITMASK(block_end_offset+1) ^ BITMASK(from_block_offset));
    METADATA_WORD(qf, tombstones, block_end_index) |= mask;
    from_index = block_end_index + 1;
    if (from_index > to_index) break;
    from_block++;
    block_end_index = MIN((from_block + 1)* QF_SLOTS_PER_BLOCK - 1, to_index);
  }
}

 #endif
