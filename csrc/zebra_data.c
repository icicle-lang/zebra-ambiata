#include "zebra_data.h"

#include <string.h>

static error_t read_int32 (const uint8_t **pp, const uint8_t *pe, int32_t *out)
{
    const uint8_t *p = *pp;

    if (p + sizeof (int32_t) > pe) return ZEBRA_NOT_ENOUGH_BYTES;

    *out = *(int32_t *)p;
    *pp = p + sizeof (int32_t);

    return ZEBRA_SUCCESS;
}

static error_t read_uint8 (const uint8_t **pp, const uint8_t *pe, uint8_t *out)
{
    const uint8_t *p = *pp;

    if (p + sizeof (uint8_t) > pe) return ZEBRA_NOT_ENOUGH_BYTES;

    *out = *(uint8_t *)p;
    *pp = p + sizeof (uint8_t);

    return ZEBRA_SUCCESS;
}

error_t zebra_alloc_table (
    anemone_mempool_t *pool
  , zebra_table_t *table
  , const uint8_t **pp_schema
  , const uint8_t *pe_schema )
{
    error_t err;

    int32_t column_count;
    err = read_int32 (pp_schema, pe_schema, &column_count);
    if (err) return err;

    zebra_column_t *columns = anemone_mempool_calloc (pool, column_count, sizeof (zebra_column_t));

    for (int32_t i = 0; i < column_count; i++) {
        uint8_t type_code;
        err = read_uint8 (pp_schema, pe_schema, &type_code);
        if (err) return err;

        switch (type_code) {
            case 'b':
                columns[i].type = ZEBRA_BYTE;
                break;

            case 'i':
                columns[i].type = ZEBRA_INT;
                break;

            case 'd':
                columns[i].type = ZEBRA_DOUBLE;
                break;

            case 'a':
                columns[i].type = ZEBRA_ARRAY;
                err = zebra_alloc_table (pool, &columns[i].data.a.table, pp_schema, pe_schema);
                if (err) return err;
                break;

            default:
                return ZEBRA_INVALID_COLUMN_TYPE;
        }
    }

    table->row_capacity = 0;
    table->column_count = column_count;
    table->columns = columns;

    return ZEBRA_SUCCESS;
}

static void* zebra_grow_array (anemone_mempool_t *pool, void *old, size_t size, int64_t old_capacity, int64_t new_capacity)
{
    // XXX: This was calloc before, have changed to alloc.
    // I don't think it needs to be zeroed since the part which is used will be initialised by memcpy,
    // and the rest is past the end.
    void *new = anemone_mempool_alloc (pool, new_capacity * size);

    //
    // Allow grow_array to do the initial allocation when there is no previous
    // array. This guard probably isn't necessary but calling memcpy with a
    // null pointer is technically undefined.
    //
    if (old) {
        memcpy (new, old, old_capacity * size);
    }

    return new;
}

error_t zebra_grow_column (anemone_mempool_t *pool, zebra_column_t *column, int64_t old_capacity, int64_t new_capacity)
{
    zebra_type_t type = column->type;
    zebra_data_t *data = &column->data;

    switch (type) {
        case ZEBRA_BYTE:
            data->b = zebra_grow_array (pool, data->b, sizeof (data->b[0]), old_capacity, new_capacity);
            return ZEBRA_SUCCESS;

        case ZEBRA_INT:
            data->i = zebra_grow_array (pool, data->i, sizeof (data->i[0]), old_capacity, new_capacity);
            return ZEBRA_SUCCESS;

        case ZEBRA_DOUBLE:
            data->d = zebra_grow_array (pool, data->d, sizeof (data->d[0]), old_capacity, new_capacity);
            return ZEBRA_SUCCESS;

        case ZEBRA_ARRAY:
            data->a.n = zebra_grow_array (pool, data->a.n, sizeof (data->a.n[0]), old_capacity, new_capacity);
            return ZEBRA_SUCCESS;

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
}

error_t zebra_grow_table (anemone_mempool_t *pool, zebra_table_t *table)
{
    int64_t row_count = table->row_count;
    int64_t row_capacity = table->row_capacity;

    if (row_count <= row_capacity) {
        //
        // We have not reached our capacity yet, so do nothing.
        //

        return ZEBRA_SUCCESS;
    }

    //
    // When the row count and the capacity are the same, it means we have
    // reached our limit and need to grow the table. If the capacity is
    // currently zero, then we need to do the initial allocation of the table.
    //

    const int64_t initial_capacity = 4;
    int64_t new_row_capacity = row_capacity == 0 ? initial_capacity : row_capacity * 2;
    table->row_capacity = new_row_capacity;

    int64_t column_count = table->column_count;
    zebra_column_t *columns = table->columns;

    error_t err;

    for (int64_t i = 0; i < column_count; i++) {
        err = zebra_grow_column (pool, columns + i, row_capacity, new_row_capacity);
        if (err) return err;
    }

    return ZEBRA_SUCCESS;
}

error_t zebra_grow_attribute (anemone_mempool_t *pool, zebra_attribute_t *attribute)
{
    zebra_table_t *table = &attribute->table;
    int64_t old_capacity = table->row_capacity;

    error_t err = zebra_grow_table (pool, table);
    if (err) return err;

    int64_t new_capacity = table->row_capacity;

    if (old_capacity < new_capacity) {
        attribute->times =
          zebra_grow_array (
              pool
            , attribute->times
            , sizeof (attribute->times[0])
            , old_capacity
            , new_capacity
            );

        attribute->priorities =
          zebra_grow_array (
              pool
            , attribute->priorities
            , sizeof (attribute->priorities[0])
            , old_capacity
            , new_capacity
            );

        attribute->tombstones =
          zebra_grow_array (
              pool
            , attribute->tombstones
            , sizeof (attribute->tombstones[0])
            , old_capacity
            , new_capacity
            );
    }

    return ZEBRA_SUCCESS;
}


error_t zebra_add_row (
    anemone_mempool_t *pool
  , zebra_entity_t *entity
  , int32_t attribute_id
  , int64_t time
  , int64_t priority
  , bool64_t tombstone
  , zebra_column_t **out_columns
  , int64_t *out_index )
{
    if (attribute_id < 0 || attribute_id >= entity->attribute_count)
        return ZEBRA_ATTRIBUTE_NOT_FOUND;

    zebra_attribute_t *attribute = entity->attributes + attribute_id;

    error_t err = zebra_grow_attribute (pool, attribute);
    if (err) return err;

    zebra_table_t *table = &attribute->table;
    zebra_column_t *columns = table->columns;
    int64_t count = table->row_count;

    *out_columns = columns;
    *out_index = count;

    int64_t *times = attribute->times;
    int64_t *priorities = attribute->priorities;
    bool64_t *tombstones = attribute->tombstones;

    times[count] = time;
    priorities[count] = priority;
    tombstones[count] = tombstone;

    table->row_count = count + 1;

    return ZEBRA_SUCCESS;
}

//
// Neritic clones (in between the shallow and the deep), here we clone table
// but keen the original reference to the data. The clone of the table's
// structure is deep, but the clone of the data is shallow.
//

ANEMONE_STATIC
error_t zebra_neritic_clone_table (
    anemone_mempool_t *pool
  , zebra_table_t *in_table
  , zebra_table_t *out_table
  );

ANEMONE_STATIC
error_t zebra_neritic_clone_column (
    anemone_mempool_t *pool
  , zebra_column_t *in_column
  , zebra_column_t *out_column )
{
    zebra_type_t type = in_column->type;

    out_column->type = type;

    zebra_data_t *in_data = &in_column->data;
    zebra_data_t *out_data = &out_column->data;

    switch (type) {
        case ZEBRA_BYTE:
            out_data->b = in_data->b;
            return ZEBRA_SUCCESS;

        case ZEBRA_INT:
            out_data->i = in_data->i;
            return ZEBRA_SUCCESS;

        case ZEBRA_DOUBLE:
            out_data->d = in_data->d;
            return ZEBRA_SUCCESS;

        case ZEBRA_ARRAY:
            out_data->a.n = in_data->a.n;
            return zebra_neritic_clone_table (pool, &in_data->a.table, &out_data->a.table);

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
}

ANEMONE_STATIC
error_t zebra_neritic_clone_columns (
    anemone_mempool_t *pool
  , int64_t column_count
  , zebra_column_t *in_columns
  , zebra_column_t **out_columns )
{
    error_t err;

    zebra_column_t *columns = anemone_mempool_calloc (pool, column_count, sizeof (zebra_column_t));

    for (int64_t i = 0; i < column_count; i++) {
        err = zebra_neritic_clone_column (pool, in_columns + i, columns + i);
        if (err) return err;
    }

    *out_columns = columns;

    return ZEBRA_SUCCESS;
}

ANEMONE_STATIC
error_t zebra_neritic_clone_table (
    anemone_mempool_t *pool
  , zebra_table_t *in_table
  , zebra_table_t *out_table )
{
    out_table->row_count = in_table->row_count;
    out_table->row_capacity = 0;

    int64_t column_count = in_table->column_count;
    out_table->column_count = column_count;

    return zebra_neritic_clone_columns (pool, column_count, in_table->columns, &out_table->columns);
}

ANEMONE_STATIC
error_t zebra_neritic_clone_tables (
    anemone_mempool_t *pool
  , int64_t table_count
  , zebra_table_t *in_tables
  , zebra_table_t **out_tables )
{
    error_t err;

    zebra_table_t *tables = anemone_mempool_alloc (pool, table_count * sizeof (zebra_table_t));

    for (int64_t i = 0; i < table_count; i++) {
        err = zebra_neritic_clone_table (pool, in_tables + i, tables + i);
        if (err) return err;
    }

    *out_tables = tables;

    return ZEBRA_SUCCESS;
}

//
// Mutably drops 'n_rows' from the input table, and returns them in the output
// table.
//

ANEMONE_STATIC
error_t zebra_table_pop_rows (
    anemone_mempool_t *pool
  , int64_t n_rows
  , zebra_table_t *in_table
  , zebra_table_t *out_table
  );

ANEMONE_STATIC
error_t zebra_column_pop_rows (
    anemone_mempool_t *pool
  , int64_t n_rows
  , zebra_column_t *in_column
  , zebra_column_t *out_column )
{
    zebra_type_t type = in_column->type;

    out_column->type = type;

    zebra_data_t *in_data = &in_column->data;
    zebra_data_t *out_data = &out_column->data;

    switch (type) {
        case ZEBRA_BYTE:
        {
            uint8_t *b = in_data->b;

            out_data->b = b;
            in_data->b = b + n_rows;

            return ZEBRA_SUCCESS;
        }

        case ZEBRA_INT:
        {
            int64_t *i = in_data->i;

            out_data->i = i;
            in_data->i = i + n_rows;

            return ZEBRA_SUCCESS;
        }

        case ZEBRA_DOUBLE:
        {
            double *d = in_data->d;

            out_data->d = d;
            in_data->d = d + n_rows;

            return ZEBRA_SUCCESS;
        }

        case ZEBRA_ARRAY:
        {
            int64_t *n = in_data->a.n;

            out_data->a.n = n;
            in_data->a.n = n + n_rows;

            int64_t n_inner_rows = 0;

            for (int64_t i = 0; i < n_rows; i++) {
                n_inner_rows += n[i];
            }

            return zebra_table_pop_rows (pool, n_inner_rows, &in_data->a.table, &out_data->a.table);
        }

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
}

ANEMONE_STATIC
error_t zebra_table_pop_rows (
    anemone_mempool_t *pool
  , int64_t n_rows
  , zebra_table_t *in_table
  , zebra_table_t *out_table )
{
    if (n_rows > in_table->row_count)
        return ZEBRA_NOT_ENOUGH_ROWS;

    in_table->row_count -= n_rows;
    out_table->row_count = n_rows;

    int64_t column_count = in_table->column_count;
    zebra_column_t *in_columns = in_table->columns;
    zebra_column_t *out_columns = anemone_mempool_calloc (pool, column_count, sizeof (zebra_column_t));

    out_table->column_count = column_count;
    out_table->columns = out_columns;

    for (int64_t i = 0; i < column_count; i++) {
        zebra_column_pop_rows (pool, n_rows, in_columns + i, out_columns + i);
    }

    return ZEBRA_SUCCESS;
}

error_t zebra_entities_of_block (
    anemone_mempool_t *pool
  , zebra_block_t *block
  , int64_t *out_entity_count
  , zebra_entity_t **out_entities )
{
    error_t err;

    int64_t table_count = block->table_count;
    zebra_table_t *tables;

    err = zebra_neritic_clone_tables (pool, table_count, block->tables, &tables);
    if (err) return err;

    int64_t block_rows_remaining = block->row_count;
    int64_t *block_times = block->times;
    int64_t *block_priorities = block->priorities;
    bool64_t *block_tombstones = block->tombstones;

    int64_t entity_count = block->entity_count;
    zebra_entity_t *entities = anemone_mempool_calloc (pool, entity_count, sizeof (zebra_entity_t));

    for (int64_t eix = 0; eix < entity_count; eix++) {
        zebra_block_entity_t *block_entity = block->entities + eix;
        zebra_entity_t *entity = entities + eix;

        entity->hash = block_entity->hash;
        entity->id_length = block_entity->id_length;
        entity->id_bytes = block_entity->id_bytes;

        int64_t attribute_count = block_entity->attribute_count;
        int64_t *attribute_ids = block_entity->attribute_ids;
        int64_t *attribute_row_counts = block_entity->attribute_row_counts;
        zebra_attribute_t *attributes = anemone_mempool_calloc (pool, attribute_count, sizeof (zebra_attribute_t));

        for (int64_t aix = 0; aix < attribute_count; aix++) {
            int64_t attribute_id = attribute_ids[aix];
            int64_t attribute_row_count = attribute_row_counts[aix];

            if (attribute_id < 0 || attribute_id >= table_count) {
                return ZEBRA_ATTRIBUTE_NOT_FOUND;
            }

            zebra_attribute_t *attribute = attributes + aix;

            attribute->times = block_times;
            attribute->priorities = block_priorities;
            attribute->tombstones = block_tombstones;

            block_times += attribute_row_count;
            block_priorities += attribute_row_count;
            block_tombstones += attribute_row_count;
            block_rows_remaining -= attribute_row_count;

            zebra_table_t *table = tables + attribute_id;

            err = zebra_table_pop_rows (pool, attribute_row_count, table, &attribute->table);
            if (err) return err;
        }

        entity->attribute_count = attribute_count;
        entity->attributes = attributes;
    }

    *out_entity_count = entity_count;
    *out_entities = entities;

    return ZEBRA_SUCCESS;
}
