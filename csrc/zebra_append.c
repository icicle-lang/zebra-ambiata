#include "zebra_append.h"
#include "zebra_clone.h"
#include "zebra_grow.h"


//
// Append: push a single value onto the end of an attribute.
// Take the value of attribute "in" at index "ix", and add it to the end of "out_into".
//
error_t zebra_append_attribute (anemone_mempool_t *pool, const zebra_attribute_t *in, int64_t ix, zebra_attribute_t *out_into, int64_t out_count)
{
    error_t err;

    int64_t out_ix = out_into->table.row_count;

    out_into->table.row_count += out_count;
    err = zebra_grow_attribute (pool, out_into);
    if (err) return err;

    err = zebra_append_table_nogrow (pool, &in->table, ix, &out_into->table, out_count);
    if (err) return err;

    memcpy (out_into->times + out_ix, in->times + ix, out_count * sizeof(int64_t));
    memcpy (out_into->priorities + out_ix, in->priorities + ix, out_count * sizeof(int64_t));
    memcpy (out_into->tombstones + out_ix, in->tombstones + ix, out_count * sizeof(bool64_t));

    return 0;
}

ANEMONE_STATIC
ANEMONE_INLINE
error_t zebra_append_column_array (anemone_mempool_t *pool, const zebra_column_t *in, int64_t in_ix, zebra_column_t *out_into, int64_t out_ix, int64_t out_count)
{
    int64_t* in_n = in->data.a.n + in_ix;
    int64_t* out_n = out_into->data.a.n + out_ix;
    int64_t in_scan_start = in_n[0];


    // copy over the segment descriptor lengths
    int64_t out_scan_start = out_n[0];

    for (int64_t ix = 0; ix != out_count; ++ix) {
        int64_t in_scan = in_n[ix + 1] - in_scan_start;
        out_n[ix + 1] = out_scan_start + in_scan;
    }

    int64_t in_offs = in->data.a.n[0];
    // how many nested values to skip in the input
    int64_t in_skip = in_scan_start - in_offs;
    // and how many to copy
    int64_t in_scan_count = in_n[out_count];
    int64_t in_copy = in_scan_count - in_scan_start;
    return zebra_append_table (pool, &in->data.a.table, in_skip, &out_into->data.a.table, in_copy);
}

ANEMONE_STATIC
ANEMONE_INLINE
error_t zebra_append_column (anemone_mempool_t *pool, const zebra_column_t *in, int64_t in_ix, zebra_column_t *out_into, int64_t out_ix, int64_t out_count)
{
    if (in->type != out_into->type) return ZEBRA_MERGE_DIFFERENT_COLUMN_TYPES;
    if (out_count == 0) return ZEBRA_SUCCESS;

    switch (in->type) {
        case ZEBRA_BYTE:
            memcpy(out_into->data.b + out_ix, in->data.b + in_ix, out_count * sizeof(uint8_t));
            return ZEBRA_SUCCESS;

        case ZEBRA_INT:
            memcpy(out_into->data.i + out_ix, in->data.i + in_ix, out_count * sizeof(uint64_t));
            return ZEBRA_SUCCESS;

        case ZEBRA_DOUBLE:
            memcpy(out_into->data.d + out_ix, in->data.d + in_ix, out_count * sizeof(double));
            return ZEBRA_SUCCESS;

        case ZEBRA_ARRAY:
            return zebra_append_column_array (pool, in, in_ix, out_into, out_ix, out_count);
        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
}

error_t zebra_append_table (anemone_mempool_t *pool, const zebra_table_t *in, int64_t in_ix, zebra_table_t *out_into, int64_t count)
{
    error_t err;

    out_into->row_count += count;
    err = zebra_grow_table (pool, out_into);
    if (err) return err;

    return zebra_append_table_nogrow (pool, in, in_ix, out_into, count);
}

error_t zebra_append_table_nogrow (anemone_mempool_t *pool, const zebra_table_t *in, int64_t in_ix, zebra_table_t *out_into, int64_t count)
{
    error_t err;

    int64_t out_ix = out_into->row_count - count;
    for (int64_t c = 0; c < in->column_count; ++c) {
        err = zebra_append_column (pool, in->columns + c, in_ix, out_into->columns + c, out_ix, count);
        if (err) return err;
    }

    return ZEBRA_SUCCESS;
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
    block->priorities = ZEBRA_ENSURE_CAPACITY (pool, block->priorities, block->row_count, new_row_count);
    block->tombstones = ZEBRA_ENSURE_CAPACITY (pool, block->tombstones, block->row_count, new_row_count);
    block->row_count = new_row_count;

    int64_t cur_row_count = old_row_count;
    for (int64_t c = 0; c != entity->attribute_count; ++c) {
        zebra_attribute_t *attribute = entity->attributes + c;
        int64_t row_count = attribute->table.row_count;

        memcpy (block->times + cur_row_count, attribute->times, row_count * sizeof(block->times[0]));
        memcpy (block->priorities + cur_row_count, attribute->priorities, row_count * sizeof(block->priorities[0]));
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
        block->tables = anemone_mempool_calloc (pool, block->table_count, sizeof(zebra_table_t) );
        for (int64_t c = 0; c < block->table_count; ++c) {
            err = zebra_deep_clone_table (pool, &entity->attributes[c].table, block->tables + c);
            if (err) return err;
        }
    }

    *inout_block = block;
    return ZEBRA_SUCCESS;
}

