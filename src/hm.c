#include "gqf.h"
#include "hm.h"
#include <algorithm>
#include <string>
#include <unordered_map>

#ifdef QF_TOMBSTONE
#include "qft.h"
#else
#include "qf.h"
#endif

#include <stdlib.h>
#include <stdio.h>

uint64_t hm_init(HM *hm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len) {
  abort();
}


bool hm_malloc(HM *hm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed, float max_load_factor) {

  int ret = qf_malloc(hm, nslots, key_bits, value_bits, hash, seed, max_load_factor);
#ifdef QF_TOMBSTONE
  reset_rebuild_cd(hm);
#endif
  return ret;
}

void hm_destroy(HM *hm) {
  qf_destroy(hm);
}

bool hm_free(HM *hm) {
  return qf_free(hm);
}

int hm_rebuild(HM *hm, uint8_t flags) {
#ifdef QF_TOMBSTONE
    qft_rebuild(hm, flags);
    return 0;
#else
    return 0;
#endif
}

int hm_insert(HM *hm, uint64_t key, uint64_t value, uint8_t flags) {

#ifdef QF_TOMBSTONE
  int ret = qft_insert(hm, key, value, flags);
  if (ret == QF_KEY_EXISTS) return ret;

#ifdef REBUILD_DEAMORTIZED_GRAVEYARD
  if (ret < 0)
    abort();
    #ifdef REBUILD_WITH_MIN_LF
    if (hm->metadata->noccupied_slots > hm->metadata->min_item_to_rebuild) {
      _deamortized_rebuild(hm);
    }
    #else
    _deamortized_rebuild(hm);
    #endif
#elif REBUILD_AT_INSERT
  if (ret < 0)
    return ret;
  _deamortized_rebuild(hm, key, flags);
#elif AMORTIZED_REBUILD
  if (ret == QF_NO_SPACE) {
    hm_rebuild(hm, flags);
    ret = qft_insert(hm, key, value, flags);
  }
  if (ret == QF_KEY_EXISTS) return ret;
  if (ret < 0) {
    // fprintf(stderr, "Insert failed: %d\n", ret);
    return ret;
  }
  if (--(hm->metadata->rebuild_cd) == 0) {
    int ret_rebuild = hm_rebuild(hm, flags);
    if (ret_rebuild < 0) {
      if (ret_rebuild == QF_NO_SPACE) {
        fprintf(stderr, "Rebuild failed: %d\n", ret_rebuild);
        return ret;
      } else return ret_rebuild;
    }
  }
#endif
  return ret;
#else
  return qf_insert(hm, key, value, flags);
#endif
}

int hm_remove(HM *hm, uint64_t key, uint8_t flags) {
#ifdef QF_TOMBSTONE
#if DELETE_AND_PUSH
  return qft_remove_push(hm, key, flags);
#else
  return qft_remove(hm, key, flags);
#endif
#else
  return qf_remove(hm, key, flags);
#endif
}

int hm_lookup(const QF *hm, uint64_t key, uint64_t *value, uint8_t flags) {
#ifdef QF_TOMBSTONE
  return qft_query(hm, key, value, flags);
#else
  return qf_lookup(hm, key, value, flags);
#endif
}

void hm_dump_metrics(const QF *qf, const std::string &dir) {
  // For each slot count the distance to nearest tombstone/free slot ahead of it.
  // For each slot count the distance to its home slot.
  std::unordered_map<uint64_t, uint64_t> hsd_count;
  std::unordered_map<uint64_t, uint64_t> tsd_count;
  std::unordered_map<uint64_t, uint64_t> cluster_count;

  uint64_t quotient = 0;
  uint64_t slot_idx = 0;
  uint64_t home_slot_distance = 0;
  uint64_t num_items_since_tombstone = 0;
  uint64_t cluster_len = 0;

  while (quotient < qf->metadata->xnslots) {
    slot_idx = std::max(quotient, slot_idx);

    if (!is_occupied(qf, quotient) && slot_idx == quotient) { // End of a cluster.
      if (cluster_len) cluster_count[cluster_len]++;
      #ifndef QF_TOMBSTONE
      // Mark pushing distance for this cluster.
      while (cluster_len) {
        // For RHM, inserting inside anywhere in a cluster,
        // requires pushing until cluster len.
        tsd_count[cluster_len]++;
        cluster_len--;
      }
      #endif
      cluster_len = 0;
    }
    // printf("%ld [%ld", quotient, slot_idx);
    // Walk all elements in this run. 
    // Also, runend cannot be a tombstone, so we can ignore that.
    while (is_occupied(qf, quotient)) {
      cluster_len++;
      #ifdef QF_TOMBSTONE
      if (is_tombstone(qf, slot_idx)) {
        // num_items seen so far.
        while(num_items_since_tombstone) {
          tsd_count[num_items_since_tombstone]++;
          num_items_since_tombstone--;
        }
      } else {
        num_items_since_tombstone++;
      }
      #endif
      home_slot_distance = slot_idx-quotient;
      hsd_count[home_slot_distance]++;
      slot_idx++;
      if (is_runend(qf, slot_idx-1)) {
        break;
      }
    } 
    //printf(" %ld]\n", slot_idx);
    quotient++;
  }


  std::string home_slot_distance_file_path = dir + "/home_slot_dist.txt";
  FILE *fd;
  fd = fopen(home_slot_distance_file_path.c_str(), "w");
  fprintf(fd, "HomeSlotDistance Count\n");
  for (auto it: hsd_count) {
    fprintf(fd, "%ld %ld\n", it.first, it.second);
  }
  fclose(fd);

  std::string tombstone_distance_path = dir + "/tombstone_dist.txt";
  fd = fopen(tombstone_distance_path.c_str(), "w");
  fprintf(fd, "TombstoneDistance Count\n");
  for (auto it: tsd_count) {
    fprintf(fd, "%ld %ld\n", it.first, it.second);
  }
  fclose(fd);

  std::string cluster_len_path = dir + "/cluster_len.txt";
  fd = fopen(cluster_len_path.c_str(), "w");
  fprintf(fd, "ClusterLen Count\n");
  for (auto it: cluster_count) {
    fprintf(fd, "%ld %ld\n", it.first, it.second);
  }
  fclose(fd);
}
