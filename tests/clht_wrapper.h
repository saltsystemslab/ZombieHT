#ifndef CLHT_HM_WRAPPER_H
#define CLHT_HM_WRAPPER_H

#include <stdint.h>
#include <string>
#include "clht_lf.h"

#define QF_NO_SPACE -1
#define QF_DOESNT_EXIST -1
#define QF_KEY_EXISTS 1

clht_t *hm;
extern inline int g_init(uint64_t nslots, uint64_t key_size, uint64_t value_size, float max_load_factor) {
	hm = clht_create(nslots/4);
	return 0;
}

extern inline int g_insert(uint64_t key, uint64_t val)
{
	clht_put(hm, key, val);
	return 0;
}

extern inline int g_lookup(uint64_t key, uint64_t *val)
{
	*val = clht_get(hm->ht, key);
	return (*val == 0) ? QF_DOESNT_EXIST : 0;
}

extern inline int g_remove(uint64_t key)
{
	clht_remove(hm, key);
	return 0;
}

extern inline int g_destroy()
{
	return 0;
}

extern inline uint64_t g_memory_usage()
{
	return clht_size_mem(hm->ht);
}

extern inline void g_dump_metrics(const std::string &dir) {
    return;
}

extern inline int g_collect_metadata_stats() {
	return 0;
}

#endif
