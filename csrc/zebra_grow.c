#include "zebra_grow.h"

error_t zebra_grow_column (anemone_mempool_t *pool, zebra_column_t *column, int64_t old_capacity, int64_t new_capacity)
{
    error_t err;

    zebra_column_variant_t *variant = &column->of;

    switch (column->tag) {
        case ZEBRA_COLUMN_UNIT:
            return ZEBRA_SUCCESS;

        case ZEBRA_COLUMN_INT:
            variant->_int.values = ZEBRA_GROW_ARRAY (pool, variant->_int.values, old_capacity, new_capacity);
            return ZEBRA_SUCCESS;

        case ZEBRA_COLUMN_DOUBLE:
            variant->_double.values = ZEBRA_GROW_ARRAY (pool, variant->_double.values, old_capacity, new_capacity);
            return ZEBRA_SUCCESS;

        case ZEBRA_COLUMN_ENUM: {
            variant->_enum.tags = ZEBRA_GROW_ARRAY (pool, variant->_enum.tags, old_capacity, new_capacity);
            int64_t c = variant->_enum.column_count;
            for (int64_t i = 0; i != c; ++i) {
                err = zebra_grow_column (pool, variant->_enum.columns[i], old_capacity, new_capacity);
                if (err) return err;
            }
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_STRUCT: {
            int64_t c = variant->_struct.column_count;
            for (int64_t i = 0; i != c; ++i) {
                err = zebra_grow_column (pool, variant->_struct.columns[i], old_capacity, new_capacity);
                if (err) return err;
            }
            return ZEBRA_SUCCESS;
        }


        case ZEBRA_COLUMN_NESTED:
            // Don't forget to keep the extra one element for the offset
            variant->_nested.indices = ZEBRA_GROW_ARRAY (pool, variant->_nested.indices, old_capacity + 1, new_capacity + 1);
            return ZEBRA_SUCCESS;

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
}

error_t zebra_grow_table (anemone_mempool_t *pool, zebra_table_t *table)
{
    error_t err;

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

    zebra_table_variant_t *variant = &table->of;

    switch (table->tag) {
        case ZEBRA_TABLE_BINARY:
            variant->_binary = ZEBRA_GROW_ARRAY (pool, variant->_binary, row_capacity, new_row_capacity);
            return ZEBRA_SUCCESS;

        case ZEBRA_TABLE_ARRAY:
            return zebra_grow_column (pool, variant->_array.values, row_capacity, new_row_capacity);

        case ZEBRA_TABLE_MAP:
            err = zebra_grow_column (pool, variant->_map.keys, row_capacity, new_row_capacity);
            if (err) return err;
            return zebra_grow_column (pool, variant->_map.values, row_capacity, new_row_capacity);

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
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

        attribute->factset_ids =
          zebra_grow_array (
              pool
            , attribute->factset_ids
            , sizeof (attribute->factset_ids[0])
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

