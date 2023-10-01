#ifndef ABSL_HM_WRAPPER_H
#define ABSL_HM_WRAPPER_H

#include <stdint.h>
#include <string>
#include "absl/container/flat_hash_map.h"

#define QF_NO_SPACE -1
absl::flat_hash_map<uint64_t, uint64_t> g_map;

extern inline int g_init(uint64_t nslots, uint64_t key_size, uint64_t value_size, float max_load_factor) {
	// Hack to make abseil reserve enough memory for nslots.
	// nslots is a power of 2, and abseil tries to allocate twice that. 
	g_map.reserve(nslots/2);
	return 0;
}

extern inline int g_insert(uint64_t key, uint64_t val)
{
	g_map.insert({key, val});
	return 0;
}

extern inline int g_lookup(uint64_t key, uint64_t *val)
{
	return g_map.contains(key);
}

extern inline int g_remove(uint64_t key)
{
	return g_map.erase(key);
}

extern inline int g_destroy()
{
	return 0;
}

extern inline uint64_t g_memory_usage()
{
	return g_map.get_size();
}

extern inline void g_dump_metrics(const std::string &dir) {
	printf("%lu %lu %f\n", g_map.size(), g_map.capacity(), g_map.load_factor());
}

extern inline int g_collect_metadata_stats() {
	return 0;
}

#endif
