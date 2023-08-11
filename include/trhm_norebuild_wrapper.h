#ifndef TRHM_NR_WRAPPER_H
#define TRHM_NR_WRAPPER_H

#include "trhm.h"

TRHM g_trobinhood_norebuild_hashmap;

extern inline int g_trhm_nr_init(uint64_t nslots, uint64_t key_size, uint64_t value_size)
{
 	// log_2(nslots) will be used as quotient bits of key_size.
	int ret = trhm_malloc(&g_trobinhood_norebuild_hashmap, nslots, key_size, value_size, QF_HASH_NONE, 0);
	g_trobinhood_norebuild_hashmap.metadata->nrebuilds = -1;
}

extern inline int g_trhm_nr_insert(uint64_t key, uint64_t val)
{
	return trhm_insert(&g_trobinhood_norebuild_hashmap, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_trhm_nr_lookup(uint64_t key, uint64_t *val)
{
	return trhm_lookup(&g_trobinhood_norebuild_hashmap, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_trhm_nr_remove(uint64_t key)
{
	return trhm_remove(&g_trobinhood_norebuild_hashmap, key, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_trhm_nr_destroy()
{
	return trhm_free(&g_trobinhood_norebuild_hashmap);
}


#endif
