#ifndef __ZEBRA_DATA_H
#define __ZEBRA_DATA_H

#if CABAL
#include "anemone_base.h"
#include "anemone_mempool.h"
#else
#include "../lib/anemone/csrc/anemone_base.h"
#include "../lib/anemone/csrc/anemone_mempool.h"
#endif

#define ZEBRA_SUCCESS 0
#define ZEBRA_INVALID_COLUMN_TYPE 30
#define ZEBRA_INVALID_ATTRIBUTE 31
#define ZEBRA_NOT_ENOUGH_BYTES 32

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
    int16_t *priorities;
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

error_t alloc_table (
    anemone_mempool_t *pool
  , zebra_table_t *table
  , const uint8_t **pp_schema
  , const uint8_t *pe_schema );

error_t add_row (
    anemone_mempool_t *pool
  , zebra_entity_t *entity
  , int32_t attribute_id
  , int64_t time
  , int16_t priority
  , bool64_t tombstone
  , zebra_column_t **out_columns
  , int64_t *out_index );


error_t grow_column (
    anemone_mempool_t *pool
  , zebra_column_t *column
  , int64_t old_capacity
  , int64_t new_capacity );

error_t grow_table (
    anemone_mempool_t *pool
  , zebra_table_t *table );

error_t grow_attribute (
    anemone_mempool_t *pool
  , zebra_attribute_t *attribute );

#endif//__ZEBRA_DATA_H
