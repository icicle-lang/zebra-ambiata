#include "zebra_data.h"
#include "zebra_merge.h"

#if CABAL
#include "anemone_memcmp.h"
#else
#include "../lib/anemone/csrc/anemone_memcmp.h"
#endif

//
// Agile clone: copy the structure, but throw away the content.
// This is used for allocating an empty entity with the same schema as an existing entity.
//
// We could probably save time by pre-allocating the contents arrays to hold both input entities,
// but at the moment we set the initial capacity to zero.
//
error_t zebra_agile_clone_attribute (anemone_mempool_t *pool, const zebra_attribute_t *attribute, zebra_attribute_t *into)
{
    into->times = NULL;
    into->priorities = NULL;
    into->tombstones = NULL;
    return zebra_agile_clone_table(pool, &attribute->table, &into->table);
}

error_t zebra_agile_clone_table (anemone_mempool_t *pool, const zebra_table_t *table, zebra_table_t *into)
{
    into->row_count = 0;
    into->row_capacity = 0;
    int64_t count = table->column_count;
    into->column_count = count;
    into->columns = anemone_mempool_calloc (pool, count, sizeof (zebra_column_t));
    for (int64_t c = 0; c < count; ++c) {
        zebra_type_t type = table->columns[c].type;
        into->columns[c].type = type;
        if (type == ZEBRA_ARRAY) {
            zebra_agile_clone_table (pool, &table->columns[c].data.a.table, &into->columns[c].data.a.table);
        }
    }
    return ZEBRA_SUCCESS;
}


//
// Append: push a single value onto the end of an attribute.
// Take the value of attribute "in" at index "ix", and add it to the end of "out_into".
//
error_t zebra_merge_append_attribute (anemone_mempool_t *pool, const zebra_attribute_t *in, int64_t ix, zebra_attribute_t *out_into)
{
    error_t err;

    int64_t out_ix = out_into->table.row_count;

    out_into->table.row_count++;
    err = zebra_grow_attribute (pool, out_into);
    if (err) return err;

    out_into->times[out_ix] = in->times[ix];
    out_into->priorities[out_ix] = in->priorities[ix];
    out_into->tombstones[out_ix] = in->tombstones[ix];

    return zebra_merge_append_table (pool, &in->table, ix, &out_into->table, out_ix);
}


error_t zebra_merge_append_column (anemone_mempool_t *pool, const zebra_column_t *in, int64_t in_ix, zebra_column_t *out_into, int64_t out_ix)
{
    error_t err;

    if (in->type != out_into->type) return ZEBRA_MERGE_DIFFERENT_COLUMN_TYPES;

    switch (in->type) {
        case ZEBRA_BYTE:
            out_into->data.b[out_ix] = in->data.b[in_ix];
            return ZEBRA_SUCCESS;

        case ZEBRA_INT:
            out_into->data.i[out_ix] = in->data.i[in_ix];
            return ZEBRA_SUCCESS;

        case ZEBRA_DOUBLE:
            out_into->data.d[out_ix] = in->data.d[in_ix];
            return ZEBRA_SUCCESS;

        case ZEBRA_ARRAY:
            {
                // find value start indices by summing array lengths
                // this could be better if we stored this somewhere rather than recomputing each time
                int64_t value_count = in->data.a.n[in_ix];
                int64_t value_in_ix = 0;
                int64_t value_out_ix = 0;
                for (int64_t i = 0; i < in_ix; ++i) {
                    value_in_ix += in->data.a.n[i];
                }
                for (int64_t o = 0; o < out_ix; ++o) {
                    value_out_ix += out_into->data.a.n[o];
                }

                out_into->data.a.n[out_ix] = value_count;

                out_into->data.a.table.row_count += value_count;
                zebra_grow_table (pool, &out_into->data.a.table);
                // copy each value separately. this could be a lot better.
                // zebra_merge_append_* should copy multiple values
                for (int64_t v = 0; v < value_count; ++v) {
                    err = zebra_merge_append_table (pool, &in->data.a.table, value_in_ix, &out_into->data.a.table, value_out_ix);
                    if (err) return err;
                    value_in_ix++;
                    value_out_ix++;
                }

                return ZEBRA_SUCCESS;
            }

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
}

error_t zebra_merge_append_table (anemone_mempool_t *pool, const zebra_table_t *in, int64_t in_ix, zebra_table_t *out_into, int64_t out_ix)
{
    error_t err;

    for (int64_t c = 0; c < in->column_count; ++c) {
        err = zebra_merge_append_column (pool, in->columns + c, in_ix, out_into->columns + c, out_ix);
        if (err) return err;
    }
    return ZEBRA_SUCCESS;
}


error_t zebra_merge_attribute (anemone_mempool_t *pool, const zebra_attribute_t *in1, const zebra_attribute_t *in2, zebra_attribute_t *out_into)
{
    error_t err;

    err = zebra_agile_clone_attribute (pool, in1, out_into);
    if (err) return err;

    int64_t in1_ix = 0;
    int64_t in2_ix = 0;

    while (in1_ix < in1->table.row_count && in2_ix < in2->table.row_count) {
        int64_t time1 = in1->times[in1_ix];
        int64_t time2 = in2->times[in2_ix];
        int64_t prio1 = in1->priorities[in1_ix];
        int64_t prio2 = in2->priorities[in2_ix];

        // ordered by time, priority. lowest priority first
        bool64_t copy_from_1 = (time1 < time2)
            || (time1 == time2 && prio1 < prio2);

        if (copy_from_1) {
            err = zebra_merge_append_attribute (pool, in1, in1_ix, out_into);
            if (err) return err;

            in1_ix++;
        } else {
            err = zebra_merge_append_attribute (pool, in2, in2_ix, out_into);
            if (err) return err;

            in2_ix++;
        }
    }

    // assert (in1_ix == in1->table.row_count || in2_ix == in2->table.row_count)
    // fixup loops after one of the inputs is finished
    while (in1_ix < in1->table.row_count) {
        err = zebra_merge_append_attribute (pool, in1, in1_ix, out_into);
        if (err) return err;

        in1_ix++;
    }
    while (in2_ix < in2->table.row_count) {
        err = zebra_merge_append_attribute (pool, in2, in2_ix, out_into);
        if (err) return err;

        in2_ix++;
    }

    return ZEBRA_SUCCESS;
}

error_t zebra_merge_entity (anemone_mempool_t *pool, const zebra_entity_t *in1, const zebra_entity_t *in2, zebra_entity_t *out_into)
{
    error_t err;

    if (in1->hash != in2->hash ||
        in1->id_length != in2->id_length ||
        in1->attribute_count != in2->attribute_count) {
        return ZEBRA_MERGE_DIFFERENT_ENTITIES;
    }
    // #if PARANOID
    // assert in1->id_length == in2->id_length
    if (anemone_memcmp(in1->id_bytes, in2->id_bytes, in1->id_length) != 0) {
        return ZEBRA_MERGE_DIFFERENT_ENTITIES;
    }
    // #endif

    out_into->hash            = in1->hash;
    out_into->id_length       = in1->id_length;
    out_into->id_bytes        = in1->id_bytes;
    out_into->attribute_count = in1->attribute_count;

    out_into->attributes = anemone_mempool_alloc (pool, sizeof (zebra_attribute_t) * out_into->attribute_count );
    for (int64_t c = 0; c < out_into->attribute_count; ++c) {
        err = zebra_merge_attribute (pool, in1->attributes + c, in2->attributes + c, out_into->attributes + c);
        if (err) return err;
    }

    return ZEBRA_SUCCESS;
}


