#include "zebra_append.h"
#include "zebra_clone.h"
#include "zebra_grow.h"


error_t zebra_append_table_nogrow (
    anemone_mempool_t *pool
  , const zebra_table_t *in
  , int64_t ix
  , zebra_table_t *out_into
  , int64_t out_count
  );

//
// Append: push a single value onto the end of an attribute.
// Take the value of attribute "in" at index "ix", and add it to the end of "out_into".
//
error_t zebra_append_attribute (anemone_mempool_t *pool, const zebra_attribute_t *in, int64_t ix, zebra_attribute_t *out_into, int64_t out_count)
{
    error_t err;

    int64_t out_ix = out_into->table.row_count;

    err = zebra_grow_attribute (pool, out_into, out_count);
    if (err) return err;

    err = zebra_append_table_nogrow (pool, &in->table, ix, &out_into->table, out_count);
    if (err) return err;

    memcpy (out_into->times + out_ix, in->times + ix, out_count * sizeof(int64_t));
    memcpy (out_into->factset_ids + out_ix, in->factset_ids + ix, out_count * sizeof(int64_t));
    memcpy (out_into->tombstones + out_ix, in->tombstones + ix, out_count * sizeof(bool64_t));

    return 0;
}

ANEMONE_STATIC
ANEMONE_INLINE
error_t zebra_append_column_nested (anemone_mempool_t *pool, const zebra_column_t *in, int64_t in_ix, zebra_column_t *out_into, int64_t out_ix, int64_t out_count)
{
    int64_t* in_n = in->of._nested.indices + in_ix;
    int64_t* out_n = out_into->of._nested.indices + out_ix;
    int64_t in_scan_start = in_n[0];


    // copy over the segment descriptor lengths
    int64_t out_scan_start = out_n[0];

    for (int64_t ix = 0; ix != out_count; ++ix) {
        // See Note: zebra_column._nested.indices
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // The indices array is one longer than the length
        int64_t in_scan = in_n[ix + 1] - in_scan_start;
        out_n[ix + 1] = out_scan_start + in_scan;
    }

    int64_t in_offs = in->of._nested.indices[0];
    // how many nested values to skip in the input
    int64_t in_skip = in_scan_start - in_offs;
    // and how many to copy
    int64_t in_scan_count = in_n[out_count];
    int64_t in_copy = in_scan_count - in_scan_start;
    return zebra_append_table (pool, &in->of._nested.table, in_skip, &out_into->of._nested.table, in_copy);
}

ANEMONE_STATIC
error_t zebra_append_column (anemone_mempool_t *pool, const zebra_column_t *in, int64_t in_ix, zebra_column_t *out_into, int64_t out_ix, int64_t out_count);

ANEMONE_STATIC
ANEMONE_INLINE
error_t zebra_append_named_columns (anemone_mempool_t *pool, const zebra_named_columns_t *in, int64_t in_ix, zebra_named_columns_t *out_into, int64_t out_ix, int64_t out_count)
{
    if (in->count != out_into->count) return ZEBRA_APPEND_DIFFERENT_COLUMN_TYPES;

    int64_t c = in->count;
    for (int64_t i = 0; i != c; ++i) {
        error_t err = zebra_append_column (pool, in->columns + i, in_ix, out_into->columns + i, out_ix, out_count);
        if (err) return err;
    }
    return ZEBRA_SUCCESS;
}

ANEMONE_STATIC
error_t zebra_append_column (anemone_mempool_t *pool, const zebra_column_t *in, int64_t in_ix, zebra_column_t *out_into, int64_t out_ix, int64_t out_count)
{
    if (in->tag != out_into->tag) return ZEBRA_APPEND_DIFFERENT_COLUMN_TYPES;

    switch (in->tag) {
        case ZEBRA_COLUMN_UNIT:
            return ZEBRA_SUCCESS;

        case ZEBRA_COLUMN_INT:
            memcpy (out_into->of._int.values + out_ix, in->of._int.values + in_ix, out_count * sizeof(int64_t));
            return ZEBRA_SUCCESS;

        case ZEBRA_COLUMN_DOUBLE:
            memcpy (out_into->of._double.values + out_ix, in->of._double.values + in_ix, out_count * sizeof(double));
            return ZEBRA_SUCCESS;

        case ZEBRA_COLUMN_ENUM:
            memcpy (out_into->of._enum.tags + out_ix, in->of._enum.tags + in_ix, out_count * sizeof(double));
            return zebra_append_named_columns (pool, &in->of._enum.columns, in_ix, &out_into->of._enum.columns, out_ix, out_count);

        case ZEBRA_COLUMN_STRUCT:
            return zebra_append_named_columns (pool, &in->of._struct.columns, in_ix, &out_into->of._struct.columns, out_ix, out_count);

        case ZEBRA_COLUMN_NESTED:
            return zebra_append_column_nested (pool, in, in_ix, out_into, out_ix, out_count);

        case ZEBRA_COLUMN_REVERSED:
            return zebra_append_column (pool, in->of._reversed.column, in_ix, out_into->of._reversed.column, out_ix, out_count);

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
}

error_t zebra_append_table (anemone_mempool_t *pool, const zebra_table_t *in, int64_t in_ix, zebra_table_t *out_into, int64_t count)
{
    error_t err = zebra_grow_table (pool, out_into, count);
    if (err) return err;

    return zebra_append_table_nogrow (pool, in, in_ix, out_into, count);
}

error_t zebra_append_table_nogrow (anemone_mempool_t *pool, const zebra_table_t *in, int64_t in_ix, zebra_table_t *out_into, int64_t count)
{
    if (in->tag != out_into->tag) return ZEBRA_APPEND_DIFFERENT_COLUMN_TYPES;
    if (count == 0) return ZEBRA_SUCCESS;

    // The table has already been grown, so the row_count includes the values we're about to append
    int64_t out_ix = out_into->row_count - count;

    switch (in->tag) {
        case ZEBRA_TABLE_BINARY: {
            memcpy (out_into->of._binary.bytes + out_ix, in->of._binary.bytes + in_ix, count * sizeof(char));
        }

        case ZEBRA_TABLE_ARRAY: {
            return zebra_append_column (pool, in->of._array.values, in_ix, out_into->of._array.values, out_ix, count);
        }

        case ZEBRA_TABLE_MAP: {
            error_t err = zebra_append_column (pool, in->of._map.keys, in_ix, out_into->of._map.keys, out_ix, count);
            if (err) return err;
            return zebra_append_column (pool, in->of._map.values, in_ix, out_into->of._map.values, out_ix, count);
        }

        default: {
            return ZEBRA_INVALID_TABLE_TYPE;
        }
    }
}

ANEMONE_STATIC
error_t zebra_fill_block_entity (anemone_mempool_t *pool, zebra_entity_t *entity, zebra_block_entity_t *block_entity)
{
    block_entity->hash = entity->hash;
    block_entity->id_length = entity->id_length;
    block_entity->id_bytes = ZEBRA_CLONE_ARRAY (pool, entity->id_bytes, entity->id_length);

    int64_t nonzeros = 0;
    for (int64_t c = 0; c != entity->attribute_count; ++c) {
        if (entity->attributes[c].table.row_count > 0)
            nonzeros++;
    }

    block_entity->attribute_count = nonzeros;
    block_entity->attribute_ids = anemone_mempool_alloc (pool, sizeof(int64_t) * nonzeros);
    block_entity->attribute_row_counts = anemone_mempool_alloc (pool, sizeof(int64_t) * nonzeros);

    int64_t sparse_ix = 0;
    for (int64_t c = 0; c != entity->attribute_count; ++c) {
        int64_t row_count = entity->attributes[c].table.row_count;
        if (row_count > 0) {
            block_entity->attribute_ids[sparse_ix] = c;
            block_entity->attribute_row_counts[sparse_ix] = row_count;
            sparse_ix++;
        }
    }

    return ZEBRA_SUCCESS;
}

// Ensure an array has the capacity to store some stuff.
// The array must have been allocated using zebra_ensure_capacity, because it adds the extra stuff on the end.
ANEMONE_STATIC
ANEMONE_INLINE
void* zebra_ensure_capacity (anemone_mempool_t *pool, void *old, size_t size, int64_t old_used, int64_t required)
{
    int64_t current_capacity = zebra_grow_array_capacity (old_used);

    if (old != NULL && required <= current_capacity) return old;
    
    return zebra_grow_array (pool, old, size, old_used, zebra_grow_array_capacity (required) );
}
#define ZEBRA_ENSURE_CAPACITY(pool, in, oldcap, newcap) zebra_ensure_capacity (pool, in, sizeof (in[0]), oldcap, newcap )

error_t zebra_append_block_entity (anemone_mempool_t *pool, zebra_entity_t *entity, zebra_block_t **inout_block)
{
    error_t err;
    zebra_block_t *block = *inout_block;

    if (!block) {
        block = anemone_mempool_calloc (pool, 1, sizeof (zebra_block_t) );
    } else if (entity->attribute_count != block->table_count) {
        return ZEBRA_APPEND_DIFFERENT_ATTRIBUTE_COUNT;
    }

    block->entities = ZEBRA_ENSURE_CAPACITY (pool, block->entities, block->entity_count, block->entity_count + 1);
    err = zebra_fill_block_entity (pool, entity, block->entities + block->entity_count);
    if (err) return err;
    block->entity_count++;

    int64_t old_row_count = block->row_count;
    int64_t new_row_count = block->row_count;
    for (int64_t c = 0; c != entity->attribute_count; ++c) {
        new_row_count += entity->attributes[c].table.row_count;
    }

    block->times = ZEBRA_ENSURE_CAPACITY (pool, block->times, block->row_count, new_row_count);
    block->factset_ids = ZEBRA_ENSURE_CAPACITY (pool, block->factset_ids, block->row_count, new_row_count);
    block->tombstones = ZEBRA_ENSURE_CAPACITY (pool, block->tombstones, block->row_count, new_row_count);
    block->row_count = new_row_count;

    int64_t cur_row_count = old_row_count;
    for (int64_t c = 0; c != entity->attribute_count; ++c) {
        zebra_attribute_t *attribute = entity->attributes + c;
        int64_t row_count = attribute->table.row_count;

        memcpy (block->times + cur_row_count, attribute->times, row_count * sizeof(block->times[0]));
        memcpy (block->factset_ids + cur_row_count, attribute->factset_ids, row_count * sizeof(block->factset_ids[0]));
        memcpy (block->tombstones + cur_row_count, attribute->tombstones, row_count * sizeof(block->tombstones[0]));

        cur_row_count += row_count;
    }

    if (block->tables) {
        for (int64_t c = 0; c < block->table_count; ++c) {
            zebra_table_t *entity_table = &entity->attributes[c].table;
            zebra_table_t *block_table = block->tables + c;
            err = zebra_append_table (pool, entity_table, 0, block_table, entity_table->row_count);
            if (err) return err;
        }
    } else {
        block->table_count = entity->attribute_count;
        block->tables = anemone_mempool_alloc (pool, block->table_count * sizeof(zebra_table_t) );
        for (int64_t c = 0; c < block->table_count; ++c) {
            err = zebra_deep_clone_table (pool, &entity->attributes[c].table, block->tables + c);
            if (err) return err;
        }
    }

    *inout_block = block;
    return ZEBRA_SUCCESS;
}

