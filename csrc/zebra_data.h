#ifndef __ZEBRA_DATA_H
#define __ZEBRA_DATA_H

#include "anemone_base.h"
#include "anemone_memcmp.h"
#include "anemone_mempool.h"

#define ZEBRA_SUCCESS 0
#define ZEBRA_INVALID_COLUMN_TYPE 30
#define ZEBRA_INVALID_TABLE_TYPE 31
#define ZEBRA_ATTRIBUTE_NOT_FOUND 32
#define ZEBRA_NOT_ENOUGH_BYTES 33
#define ZEBRA_NOT_ENOUGH_ROWS 34
#define ZEBRA_MERGE_NO_ENTITIES 35
#define ZEBRA_APPEND_DIFFERENT_COLUMN_TYPES 36
#define ZEBRA_APPEND_DIFFERENT_ATTRIBUTE_COUNT 37

typedef int64_t bool64_t;

// Forward declarations for recursive structures
struct zebra_column;
typedef struct zebra_column zebra_column_t;

// ------------------------
// Zebra.Table.Table
// ------------------------
typedef enum zebra_table_tag {
    ZEBRA_TABLE_BINARY,
    ZEBRA_TABLE_ARRAY,
    ZEBRA_TABLE_MAP,
} zebra_table_tag_t;

typedef union zebra_table_variant {
    // ZEBRA_TABLE_BINARY
    struct {
        char* bytes;
    } _binary;
    // ZEBRA_TABLE_ARRAY
    struct {
        zebra_column_t* values;
    } _array;
    // ZEBRA_TABLE_MAP
    struct {
        zebra_column_t* keys;
        zebra_column_t* values;
    } _map;
} zebra_table_variant_t;

typedef struct zebra_table {
    int64_t row_count;
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Note: zebra_table.row_capacity
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // This should *usually* be a power of two, but not always.
    // To compute the ideal row_capacity, use "zebra_grow_array_capacity (row_count)".
    // For example, zebra_neritic_clone_table sets the row_capacity = row_count to signify the array must be copied if it needs to grow.
    int64_t row_capacity;
    zebra_table_tag_t tag;
    zebra_table_variant_t of;
} zebra_table_t;


// ------------------------
// Vector (Variant, Column)
// Vector (Field,   Column)
// ------------------------
typedef struct zebra_named_columns {
    int64_t count;
    zebra_column_t *columns;
    // name_lengths_sum == name_length[0..count)
    int64_t *name_lengths;
    int64_t name_lengths_sum;
    // let indices = scan name_lengths
    // forall i. FIELD_NAME[i] = name_bytes[ indices[i] .. indices[i+1] )
    char *name_bytes;
} zebra_named_columns_t;


// ------------------------
// Zebra.Table.Column
// ------------------------
typedef enum zebra_column_tag {
    ZEBRA_COLUMN_UNIT,
    ZEBRA_COLUMN_INT,
    ZEBRA_COLUMN_DOUBLE,
    ZEBRA_COLUMN_ENUM,
    ZEBRA_COLUMN_STRUCT,
    ZEBRA_COLUMN_NESTED,
    ZEBRA_COLUMN_REVERSED,
} zebra_column_tag_t;

typedef union zebra_column_variant {
    struct {
    } _unit;
    struct {
        int64_t *values;
    } _int;
    struct {
        double *values;
    } _double;
    struct {
        int64_t *tags;
        zebra_named_columns_t columns;
    } _enum;
    struct {
        zebra_named_columns_t columns;
    } _struct;
    struct {
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // Note: zebra_column._nested.indices
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
        int64_t *indices;
        zebra_table_t table;
    } _nested;
    struct {
        zebra_column_t *column;
    } _reversed;
} zebra_column_variant_t;

struct zebra_column {
    zebra_column_tag_t tag;
    zebra_column_variant_t of;
}; // zebra_column_t


// ------------------------
// Attributes and entities
// ------------------------
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

#endif//__ZEBRA_DATA_H
