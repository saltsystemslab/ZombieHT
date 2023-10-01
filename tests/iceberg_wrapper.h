#ifndef ICEBERG_HM_WRAPPER_H
#define ICEBERG_HM_WRAPPER_H

#include <stdint.h>
#include <string>
#include "iceberg_table.h"

#define QF_NO_SPACE -1

iceberg_table ice;

extern inline int g_init(uint64_t nslots, uint64_t key_size, uint64_t value_size, float max_load_factor) {
    iceberg_init(&ice, nslots);
	return 0;
}

extern inline int g_insert(uint64_t key, uint64_t val)
{
    return iceberg_insert(&ice, key, val, 0);
}

extern inline int g_lookup(uint64_t key, uint64_t *val)
{
    return iceberg_get_value(&ice, key, val, 0);
}

extern inline int g_remove(uint64_t key)
{
    return iceberg_remove(&ice, key, 0);
}

extern inline int g_destroy()
{
	return 0;
}

extern inline uint64_t g_memory_usage()
{
    return ice.metadata.total_size_in_bytes;
}

extern inline void g_dump_metrics(const std::string &dir) {
    printf("L1:%lu L2:%lu L3: %lu\n ", lv1_balls(&ice), lv2_balls(&ice), lv3_balls(&ice));
    return;
}

extern inline int g_collect_metadata_stats() {
	return 0;
}

#endif
