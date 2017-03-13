#include "zebra_block_split.h"
#include "zebra_clone.h"

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
  , zebra_column_t *out_column
  );

ANEMONE_STATIC
ANEMONE_INLINE
error_t zebra_named_columns_pop_rows (
    anemone_mempool_t *pool
  , int64_t n_rows
  , zebra_named_columns_t *in
  , zebra_named_columns_t *out )
{
    int64_t c = in->count;

    out->count = c;
    out->columns = anemone_mempool_alloc (pool, c * sizeof (zebra_column_t) );
    out->name_lengths = in->name_lengths;
    out->name_lengths_sum = in->name_lengths_sum;
    out->name_bytes = in->name_bytes;

    for (int64_t i = 0; i != c; ++i) {
        error_t err = zebra_column_pop_rows (pool, n_rows, in->columns + i, out->columns + i);
        if (err) return err;
    }

    return ZEBRA_SUCCESS;
}

ANEMONE_STATIC
error_t zebra_column_pop_rows (
    anemone_mempool_t *pool
  , int64_t n_rows
  , zebra_column_t *in_column
  , zebra_column_t *out_column )
{
    zebra_column_tag_t tag = in_column->tag;

    out_column->tag = tag;

    zebra_column_variant_t *in_data = &in_column->of;
    zebra_column_variant_t *out_data = &out_column->of;

    switch (tag) {
        case ZEBRA_COLUMN_UNIT: {
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_INT: {
            int64_t *i = in_data->_int.values;
            out_data->_int.values = i;
            in_data->_int.values = i + n_rows;

            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_DOUBLE: {
            double *d = in_data->_double.values;
            out_data->_double.values = d;
            in_data->_double.values = d + n_rows;

            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_ENUM: {
            int64_t *tags = in_data->_enum.tags;
            out_data->_enum.tags = tags;
            in_data->_enum.tags = tags + n_rows;

            return zebra_named_columns_pop_rows (pool, n_rows, &in_data->_enum.columns, &out_data->_enum.columns);
        }

        case ZEBRA_COLUMN_STRUCT: {
            return zebra_named_columns_pop_rows (pool, n_rows, &in_data->_struct.columns, &out_data->_struct.columns);
        }

        case ZEBRA_COLUMN_NESTED: {
            int64_t *n = in_data->_nested.indices;
            out_data->_nested.indices = n;
            in_data->_nested.indices = n + n_rows;
            int64_t n_inner_rows = n[n_rows] - n[0];

            return zebra_table_pop_rows (pool, n_inner_rows, &in_data->_nested.table, &out_data->_nested.table);
        }

        case ZEBRA_COLUMN_REVERSED: {
            out_data->_reversed.column = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );
            return zebra_column_pop_rows (pool, n_rows, in_data->_reversed.column, out_data->_reversed.column);
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
    out_table->row_capacity = n_rows;
    out_table->tag = in_table->tag;

    switch (in_table->tag) {
        case ZEBRA_TABLE_BINARY: {
            char *bytes = in_table->of._binary.bytes;
            out_table->of._binary.bytes = bytes;
            in_table->of._binary.bytes = bytes + n_rows;

            return ZEBRA_SUCCESS;
        }

        case ZEBRA_TABLE_ARRAY: {
            out_table->of._array.values = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );
            return zebra_column_pop_rows (pool, n_rows, in_table->of._array.values, out_table->of._array.values);
        }

        case ZEBRA_TABLE_MAP: {
            out_table->of._map.keys = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );
            out_table->of._map.values = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );
            error_t err = zebra_column_pop_rows (pool, n_rows, in_table->of._map.keys, out_table->of._map.keys);
            if (err) return err;
            return zebra_column_pop_rows (pool, n_rows, in_table->of._map.values, out_table->of._map.values);
        }

        default: {
            return ZEBRA_INVALID_COLUMN_TYPE;
        }
    }
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
    int64_t *block_factset_ids = block->factset_ids;
    bool64_t *block_tombstones = block->tombstones;

    int64_t entity_count = block->entity_count;
    zebra_entity_t *entities = anemone_mempool_alloc (pool, entity_count * sizeof (zebra_entity_t));

    for (int64_t eix = 0; eix < entity_count; eix++) {
        zebra_block_entity_t *block_entity = block->entities + eix;
        zebra_entity_t *entity = entities + eix;

        entity->hash = block_entity->hash;
        entity->id_length = block_entity->id_length;
        entity->id_bytes = block_entity->id_bytes;

        int64_t attribute_count = block_entity->attribute_count;
        int64_t *attribute_ids = block_entity->attribute_ids;
        int64_t *attribute_row_counts = block_entity->attribute_row_counts;
        zebra_attribute_t *attributes = anemone_mempool_alloc (pool, table_count * sizeof (zebra_attribute_t));

        int64_t aix = 0;
        for (int64_t attribute_id = 0; attribute_id < table_count; attribute_id++) {
            int64_t attribute_row_count = 0;

            if (aix < attribute_count && attribute_ids[aix] == attribute_id) {
                attribute_row_count = attribute_row_counts[aix];
                aix++;
            }

            zebra_attribute_t *attribute = attributes + attribute_id;

            attribute->times = block_times;
            attribute->factset_ids = block_factset_ids;
            attribute->tombstones = block_tombstones;

            block_times += attribute_row_count;
            block_factset_ids += attribute_row_count;
            block_tombstones += attribute_row_count;
            block_rows_remaining -= attribute_row_count;

            zebra_table_t *table = tables + attribute_id;

            err = zebra_table_pop_rows (pool, attribute_row_count, table, &attribute->table);
            if (err) return err;
        }

        //
        // We need to use all the input attributes, otherwise something is wrong.
        // They are out of order or refer to a non-existent attribute.
        //
        if (aix != attribute_count) {
            return ZEBRA_ATTRIBUTE_NOT_FOUND;
        }

        entity->attribute_count = table_count;
        entity->attributes = attributes;
    }

    *out_entity_count = entity_count;
    *out_entities = entities;

    return ZEBRA_SUCCESS;
}

