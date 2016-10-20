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

error_t alloc_table (
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
                err = alloc_table (pool, &columns[i].data.a.table, pp_schema, pe_schema);
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

static void* grow_array (anemone_mempool_t *pool, void *old, size_t size, int64_t old_capacity, int64_t new_capacity)
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

error_t grow_column (anemone_mempool_t *pool, zebra_column_t *column, int64_t old_capacity, int64_t new_capacity)
{
    zebra_type_t type = column->type;
    zebra_data_t *data = &column->data;

    switch (type) {
        case ZEBRA_BYTE:
            data->b = grow_array (pool, data->b, sizeof (data->b[0]), old_capacity, new_capacity);
            return ZEBRA_SUCCESS;

        case ZEBRA_INT:
            data->i = grow_array (pool, data->i, sizeof (data->i[0]), old_capacity, new_capacity);
            return ZEBRA_SUCCESS;

        case ZEBRA_DOUBLE:
            data->d = grow_array (pool, data->d, sizeof (data->d[0]), old_capacity, new_capacity);
            return ZEBRA_SUCCESS;

        case ZEBRA_ARRAY:
            data->a.n = grow_array (pool, data->a.n, sizeof (data->a.n[0]), old_capacity, new_capacity);
            return ZEBRA_SUCCESS;

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
}

error_t grow_table (anemone_mempool_t *pool, zebra_table_t *table)
{
    int64_t row_count = table->row_count;
    int64_t row_capacity = table->row_capacity;

    if (row_count != row_capacity) {
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

    int64_t column_count = table->column_count;
    zebra_column_t *columns = table->columns;

    error_t err;

    for (int64_t i = 0; i < column_count; i++) {
        err = grow_column (pool, columns + i, row_capacity, new_row_capacity);
        if (err) return err;
    }

    return ZEBRA_SUCCESS;
}

error_t grow_attribute (anemone_mempool_t *pool, zebra_attribute_t *attribute)
{
    zebra_table_t *table = &attribute->table;
    int64_t old_capacity = table->row_capacity;

    error_t err = grow_table (pool, table);
    if (err) return err;

    int64_t new_capacity = table->row_capacity;

    if (old_capacity != new_capacity) {
        attribute->times =
          grow_array (
              pool
            , attribute->times
            , sizeof (attribute->times[0])
            , old_capacity
            , new_capacity
            );

        attribute->priorities =
          grow_array (
              pool
            , attribute->priorities
            , sizeof (attribute->priorities[0])
            , old_capacity
            , new_capacity
            );

        attribute->tombstones =
          grow_array (
              pool
            , attribute->tombstones
            , sizeof (attribute->tombstones[0])
            , old_capacity
            , new_capacity
            );
    }

    return ZEBRA_SUCCESS;
}


error_t add_row (
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
        return ZEBRA_INVALID_ATTRIBUTE;

    zebra_attribute_t *attribute = entity->attributes + attribute_id;

    error_t err = grow_attribute (pool, attribute);
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
