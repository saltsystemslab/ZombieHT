#ifndef CUCKOO_WRAPPER_H
#define CUCKOO_WRAPPER_H

#include <stdint.h>
#include <string>
#include "libcuckoo/cuckoohash_map.hh"

#define QF_NO_SPACE -1
libcuckoo::cuckoohash_map<uint64_t, uint64_t> table;

extern inline int g_init(uint64_t nslots, uint64_t key_size, uint64_t value_size, float max_load_factor) {
	table.reserve(nslots);
	table.minimum_load_factor(0.98);
	return 0;
}

extern inline int g_insert(uint64_t key, uint64_t val)
{
	table.insert(key, val);
	return 0;
}

extern inline int g_lookup(uint64_t key, uint64_t *val)
{
	try {
		*val = table.find(key);
		return 1;
	} catch (std::out_of_range e) {
		return 0;
	}
}

extern inline int g_remove(uint64_t key)
{
	table.erase(key);
	return 0;
}

extern inline int g_destroy()
{
	return 0;
}

extern inline uint64_t g_memory_usage()
{
	return table.size_in_bytes();
}

extern inline void g_dump_metrics(const std::string &dir) {
    return;
}

extern inline int g_collect_metadata_stats() {
	return 0;
}


#endif
