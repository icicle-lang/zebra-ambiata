#ifndef __ZEBRA_DATA_H
#define __ZEBRA_DATA_H

#include "anemone_base.h"
#include "anemone_memcmp.h"
#include "anemone_mempool.h"

#define ZEBRA_SUCCESS 0
#define ZEBRA_INVALID_COLUMN_TYPE 30
#define ZEBRA_ATTRIBUTE_NOT_FOUND 31
#define ZEBRA_NOT_ENOUGH_BYTES 32
#define ZEBRA_NOT_ENOUGH_ROWS 33
#define ZEBRA_MERGE_DIFFERENT_COLUMN_TYPES 34
#define ZEBRA_MERGE_NO_ENTITIES 35
#define ZEBRA_APPEND_DIFFERENT_ATTRIBUTE_COUNT 36

typedef int64_t bool64_t;

typedef enum zebra_type {
    ZEBRA_BYTE,
    ZEBRA_INT,
    ZEBRA_DOUBLE,
    ZEBRA_ARRAY,
} zebra_type_t;

struct zebra_column;
typedef struct zebra_column zebra_column_t;

typedef struct zebra_table {
    int64_t row_count;
    int64_t row_capacity;
    int64_t column_count;
    zebra_column_t *columns;
} zebra_table_t;

typedef union zebra_data {
    uint8_t *b;
    int64_t *i;
    double *d;
    struct {
        // Lengths are stored as an prefix sum of length + 1.
        // Usually, a prefix sum would always start with 0, but we
        // generalise this to allow any offset at the start.
        // This requires a little more computation to remove the offset,
        // but allows us to reuse prefix sums from the middle of other arrays
        // without copying them.
        // 
        // Example nested array:
        //
        //            [ [ 1 2 3 ]  [ 4 5 ]  [ 6 7 8 ] ]
        //  Lengths   [ 3          2        3         ]
        //  Starts    [ 0          3        5         ]
        //  Ends      [ 3          5        8         ]
        //
        // So we would represent this as follows, with "o" for the original offset.
        //  Scans     [ o        o+3      o+5     o+8 ]
        //
        // The equivalences are something like
        //
        //  Lengths[i] = Scans[i+1] - Scans[i]
        //  Starts[i]  = Scans[i]   - Scans[0]
        //  Ends[i]    = Scans[i+1] - Scans[0]
        // 
        // The inner table's row count is the total number of elements:
        //  table.row_count = Scans[length] - Scans[0]
        int64_t *n;
        zebra_table_t table;
    } a;
} zebra_data_t;

struct zebra_column {
    zebra_type_t type;
    zebra_data_t data;
}; // zebra_column_t

typedef struct zebra_attribute {
    int64_t *times;
    int64_t *factset_ids;
    bool64_t *tombstones;
    zebra_table_t table;
} zebra_attribute_t;

typedef struct zebra_entity {
    uint32_t hash;
    int64_t id_length;
    uint8_t *id_bytes;

    int64_t attribute_count;
    zebra_attribute_t *attributes;
} zebra_entity_t;

typedef struct zebra_block_entity {
    uint32_t hash;
    int64_t id_length;
    uint8_t *id_bytes;

    int64_t attribute_count;
    int64_t *attribute_ids;
    int64_t *attribute_row_counts;
} zebra_block_entity_t;

typedef struct zebra_block {
    int64_t entity_count;
    zebra_block_entity_t *entities;

    int64_t row_count;
    int64_t *times;
    int64_t *factset_ids;
    bool64_t *tombstones;

    int64_t table_count;
    zebra_table_t *tables;
} zebra_block_t;

error_t zebra_alloc_table (
    anemone_mempool_t *pool
  , zebra_table_t *table
  , const uint8_t **pp_schema
  , const uint8_t *pe_schema
  );

error_t zebra_add_row (
    anemone_mempool_t *pool
  , zebra_entity_t *entity
  , int32_t attribute_id
  , int64_t time
  , int64_t factset_id
  , bool64_t tombstone
  , zebra_column_t **out_columns
  , int64_t *out_index
  );

#endif//__ZEBRA_DATA_H
