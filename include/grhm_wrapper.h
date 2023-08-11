#ifndef GRHM_WRAPPER_H
#define GRHM_WRAPPER_H

#include "grhm.h"

GRHM g_grobinhood_hashmap;

extern inline int g_grhm_init(uint64_t nslots, uint64_t key_size, uint64_t value_size)
{
 	// log_2(nslots) will be used as quotient bits of key_size.
	return grhm_malloc(&g_grobinhood_hashmap, nslots, key_size, value_size, QF_HASH_NONE, 0);
}

extern inline int g_grhm_insert(uint64_t key, uint64_t val)
{
	return grhm_insert(&g_grobinhood_hashmap, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_grhm_lookup(uint64_t key, uint64_t *val)
{
	return grhm_lookup(&g_grobinhood_hashmap, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_grhm_remove(uint64_t key)
{
	return grhm_remove(&g_grobinhood_hashmap, key, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_grhm_destroy()
{
	return grhm_free(&g_grobinhood_hashmap);
}


#endif
