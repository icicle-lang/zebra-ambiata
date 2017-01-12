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
            int64_t n_inner_rows = n[n_rows] - n[0];

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
    out_table->row_capacity = n_rows;

    int64_t column_count = in_table->column_count;
    zebra_column_t *in_columns = in_table->columns;
    zebra_column_t *out_columns = anemone_mempool_alloc (pool, column_count * sizeof (zebra_column_t));

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

