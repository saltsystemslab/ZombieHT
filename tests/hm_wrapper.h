#ifndef HM_WRAPPER_H
#define HM_WRAPPER_H

#include "hm.h"

// TODO: Remove this file, it's confusing as to why we need it.
// I think the idea was to remove the boilerplate around initialization of the QF.

HM g_hashmap;

extern inline int g_init(uint64_t nslots, uint64_t key_size, uint64_t value_size, float max_load_factor)
{
 	// log_2(nslots) will be used as quotient bits of key_size.
	return hm_malloc(&g_hashmap, nslots, key_size, value_size, QF_HASH_NONE, 0, max_load_factor);
}

extern inline int g_insert(uint64_t key, uint64_t val)
{
	return hm_insert(&g_hashmap, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_lookup(uint64_t key, uint64_t *val)
{
	return hm_lookup(&g_hashmap, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_remove(uint64_t key)
{
	return hm_remove(&g_hashmap, key, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_destroy()
{
	return hm_free(&g_hashmap);
}

extern inline void g_dump_metrics(const std::string &dir) {
  hm_dump_metrics(&g_hashmap, dir);
}

extern inline int g_collect_metadata_stats() {
  return g_hashmap.metadata->nslots;
}

#endif
