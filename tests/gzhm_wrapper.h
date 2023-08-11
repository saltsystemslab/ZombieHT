#ifndef GZHM_WRAPPER_H
#define GZHM_WRAPPER_H

#include "gzhm.h"

GZHM g_gzhm;

extern inline int g_gzhm_init(uint64_t nslots, uint64_t key_size, uint64_t value_size)
{
 	// log_2(nslots) will be used as quotient bits of key_size.
	return gzhm_malloc(&g_gzhm, nslots, key_size, value_size, QF_HASH_NONE, 0);
}

extern inline int g_gzhm_insert(uint64_t key, uint64_t val)
{
	return gzhm_insert(&g_gzhm, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_gzhm_lookup(uint64_t key, uint64_t *val)
{
	return gzhm_lookup(&g_gzhm, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_gzhm_remove(uint64_t key)
{
	return gzhm_remove(&g_gzhm, key, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_gzhm_rebuild()
{
	return gzhm_rebuild(&g_gzhm, QF_NO_LOCK);
}

extern inline int g_gzhm_destroy()
{
	return gzhm_free(&g_gzhm);
}


#endif // GZHM_WRAPPER_H
