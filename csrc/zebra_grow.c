#include "zebra_grow.h"

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
            // Don't forget to keep the extra one element for the offset
            data->a.n = zebra_grow_array (pool, data->a.n, sizeof (data->a.n[0]), old_capacity + 1, new_capacity + 1);
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
    // When the row count is larger or equal to the capacity, it means we have
    // reached our limit and need to grow the table.
    //

    int64_t new_row_capacity = zebra_grow_array_capacity(row_count);
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

