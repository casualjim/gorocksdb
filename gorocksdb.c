#include <memory.h>
#include "gorocksdb.h"
#include "roaring.h"
#include "rocksdb/c.h"
#include "_cgo_export.h"

/* Base */

void gorocksdb_destruct_handler(void* state) { }

/* Comparator */
rocksdb_comparator_t* gorocksdb_comparator_create(uintptr_t idx) {
    return rocksdb_comparator_create(
        (void*)idx,
        gorocksdb_destruct_handler,
        (int (*)(void*, const char*, size_t, const char*, size_t))(gorocksdb_comparator_compare),
        (const char *(*)(void*))(gorocksdb_comparator_name));
}

/* CompactionFilter */

rocksdb_compactionfilter_t* gorocksdb_compactionfilter_create(uintptr_t idx) {
    return rocksdb_compactionfilter_create(
        (void*)idx,
        gorocksdb_destruct_handler,
        (unsigned char (*)(void*, int, const char*, size_t, const char*, size_t, char**, size_t*, unsigned char*))(gorocksdb_compactionfilter_filter),
        (const char *(*)(void*))(gorocksdb_compactionfilter_name));
}

/* Filter Policy */

rocksdb_filterpolicy_t* gorocksdb_filterpolicy_create(uintptr_t idx) {
    return rocksdb_filterpolicy_create(
        (void*)idx,
        gorocksdb_destruct_handler,
        (char* (*)(void*, const char* const*, const size_t*, int, size_t*))(gorocksdb_filterpolicy_create_filter),
        (unsigned char (*)(void*, const char*, size_t, const char*, size_t))(gorocksdb_filterpolicy_key_may_match),
        gorocksdb_filterpolicy_delete_filter,
        (const char *(*)(void*))(gorocksdb_filterpolicy_name));
}

void gorocksdb_filterpolicy_delete_filter(void* state, const char* v, size_t s) {
    free((char*)v);
}

/* Merge Operator */

rocksdb_mergeoperator_t* gorocksdb_mergeoperator_create(uintptr_t idx) {
    return rocksdb_mergeoperator_create(
        (void*)idx,
        gorocksdb_destruct_handler,
        (char* (*)(void*, const char*, size_t, const char*, size_t, const char* const*, const size_t*, int, unsigned char*, size_t*))(gorocksdb_mergeoperator_full_merge),
        (char* (*)(void*, const char*, size_t, const char* const*, const size_t*, int, unsigned char*, size_t*))(gorocksdb_mergeoperator_partial_merge_multi),
        gorocksdb_mergeoperator_delete_value,
        (const char* (*)(void*))(gorocksdb_mergeoperator_name));
}

void gorocksdb_mergeoperator_delete_value(void* id, const char* v, size_t s) {
    free((char*)v);
}

/* Slice Transform */

rocksdb_slicetransform_t* gorocksdb_slicetransform_create(uintptr_t idx) {
    return rocksdb_slicetransform_create(
        (void*)idx,
        gorocksdb_destruct_handler,
        (char* (*)(void*, const char*, size_t, size_t*))(gorocksdb_slicetransform_transform),
        (unsigned char (*)(void*, const char*, size_t))(gorocksdb_slicetransform_in_domain),
        (unsigned char (*)(void*, const char*, size_t))(gorocksdb_slicetransform_in_range),
        (const char* (*)(void*))(gorocksdb_slicetransform_name));
}

/* Netflix SonarDB helpers */

static int compare_timerange_bytes(void* c, const char* left, size_t szl, const char* right, size_t szr) {
    // check lengths
    if (szl == 0 && szr == 0) {
        return 0;
    }
    if (szl == 0 || (szl == 4 && szr == 8)) {
        return -1;
    }
    if (szr == 0 || (szr == 4 && szl == 8)) {
        return 1;
    }

    // if the lengths differ, they are likely to be 4 and 8. Shortcut the ones that have length 4
    int cmp1 = memcmp(right, left, 4);
    if (cmp1 != 0 || (szr ==4 && szl == 4)) {
        return cmp1;
    }
    if (szr == 4) {
        return -1;
    }
    if (szl == 4) {
        return 1;
    }
    return memcmp(&right[4], &left[4], 4);
}

static int compare_netele_versions(void* c, const char* left, size_t szl, const char* right, size_t szr) {
    // These first 3 cases should not actually occur
    if (szr == 0 && szl == 0) {
        return 0;
    }
    if (szl == 0) {
        return -1;
    }
    if (szr == 0) {
        return 1;
    }

    // compare the actual ids
    int cmp1 = memcmp(left, right, 4);
    if (cmp1 != 0 || szl == 4) {
        return cmp1;
    }
    if (szl == 4 && szr == 4) {
        return 0;
    }
    if (szl == 4) {
        return 1;
    }
    if (szr == 4) {
        return -1;
    }

    // compare start timestamps
    int cmp2 = memcmp(&right[4], &left[4], 4);
    if (cmp2 != 0) {
        return cmp2;
    }

    if (szr == 8 && szl == 8) {
        return 0;
    }
    if (szl == 8) {
        return 1;
    }
    if (szr == 8) {
        return -1;
    }
    // finally compare the end timestamps
    return memcmp(&right[8], &left[8], 4);
}

static const char* timerange_comparator_name(void* v) {
    return TIMERANGE_COMPARATOR_NAME;
}

static const char* netele_version_comparator_name(void* v) {
    return NETELE_VERSION_COMPARATOR_NAME;
}

rocksdb_comparator_t* nflx_timerange_comparator() {
    return rocksdb_comparator_create(
        NULL,
        NULL,
        compare_timerange_bytes,
        timerange_comparator_name);
}

rocksdb_comparator_t* nflx_netele_comparator() {
    return rocksdb_comparator_create(
        NULL,
        NULL,
        compare_netele_versions,
        netele_version_comparator_name);
}

typedef struct bitmap_merge_operator {
    char* name;
} bitmap_merge_operator;

char* merge_operator_full_merge_fn (
    void *state,
    const char *key, size_t key_length,
    const char *existing_value, size_t existing_value_length,
    const char *const *operands_list, const size_t *operands_list_length,
    int num_operands,
    unsigned char *success, size_t *new_value_length)
{
    roaring_bitmap_t *bmm;
    if (existing_value_length > 0) {
        bmm = roaring_bitmap_portable_deserialize(existing_value);
    } else {
        bmm = roaring_bitmap_create();
    }

    for (int i = 0; i < num_operands; i++) {
      const char *op = operands_list[i];
        switch (op[0]) {
        case 1:;
            uint32_t id_to_add = (op[1] << 24) + (op[2]<<16) + (op[3] << 8) + op[4];
            roaring_bitmap_add(bmm, id_to_add);
            *success = 1;
            break;
        case 2:;
            uint32_t id_to_remove = (op[1] << 24) + (op[2]<<16) + (op[3] << 8) + op[4];
            roaring_bitmap_remove(bmm, id_to_remove);
            *success = 1;
            break;
        }
    }

    if (roaring_bitmap_is_empty(bmm)) {
        roaring_bitmap_free(bmm);
        new_value_length = 0;
        return NULL;
    }

    size_t len = roaring_bitmap_portable_size_in_bytes(bmm);
    char *result = malloc(len);
    roaring_bitmap_portable_serialize(bmm, result);
    roaring_bitmap_free(bmm);
    *success = 1;
    *new_value_length = len;
    return result;
}

const char* merge_operator_name_fn(void *state)
{
    return ((bitmap_merge_operator*)state)->name;
}

void merge_operator_destructor_fn(void *state)
{
    free(state);
}

rocksdb_mergeoperator_t* nflx_bitmap_merger(char *name) {
    struct bitmap_merge_operator *state = malloc(sizeof(*state));
    if (!state) {
    return NULL;
    }

    state->name = name;
    return rocksdb_mergeoperator_create(
        (void *)state,
        merge_operator_destructor_fn,
        merge_operator_full_merge_fn,
        NULL,
        NULL,
        merge_operator_name_fn);
}

