#include "zebra_data.h"

error_t merge_append_table (zebra_table_t *in, int64_t ix, zebra_table_t *out, int64_t out_ix);

error_t merge_append_column (zebra_column_t *in, int64_t in_ix, zebra_column_t *out, int64_t out_ix)
{
    error_t err;

    // todo better error
    if (in->type != out->type) return ZEBRA_INVALID_COLUMN_TYPE;

    switch (in->type) {
        case ZEBRA_BYTE:
            out->data.b[out_ix] = in->data.b[in_ix];
            return ZEBRA_SUCCESS;

        case ZEBRA_INT:
            out->data.i[out_ix] = in->data.i[in_ix];
            return ZEBRA_SUCCESS;

        case ZEBRA_DOUBLE:
            out->data.d[out_ix] = in->data.d[in_ix];
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
                    value_out_ix += out->data.a.n[o];
                }

                out->data.a.n[out_ix] = value_count;
                // copy each value separately. this could be a lot better.
                // merge_append_* should copy multiple values & grow once at start.
                for (int64_t v = 0; v < value_count; ++v) {
                    out->data.a.table.row_count++;
                    grow_table (&out->data.a.table);

                    err = merge_append_table (&in->data.a.table, value_in_ix, &out->data.a.table, value_out_ix);
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

error_t merge_append_table (zebra_table_t *in, int64_t in_ix, zebra_table_t *out, int64_t out_ix)
{
    error_t err;

    for (int64_t c = 0; c < in->column_count; ++c) {
        err = merge_append_column (in->columns + c, in_ix, out->columns + c, out_ix);
        if (err) return err;
    }
    return ZEBRA_SUCCESS;
}

error_t merge_append_attribute (zebra_attribute_t *in, int64_t ix, zebra_attribute_t *out)
{
    error_t err;

    int64_t out_ix = out->table.row_count;

    out->table.row_count++;
    err = grow_attribute (out);
    if (err) return err;

    out->times[out_ix] = in->times[ix];
    out->priorities[out_ix] = in->priorities[ix];
    out->tombstones[out_ix] = in->tombstones[ix];

    return merge_append_table (&in->table, ix, &out->table, out_ix);
}

error_t empty_attribute (anemone_mempool_t *pool, zebra_attribute_t *in, zebra_attribute_t **out);

error_t merge_attribute (anemone_mempool_t *pool, zebra_attribute_t *in1, zebra_attribute_t *in2, zebra_attribute_t **out)
{
    error_t err;

    err = empty_attribute (pool, in1, out);
    if (err) return err;

    zebra_attribute_t *attr = *out;

    int64_t in1_ix = 0;
    int64_t in2_ix = 0;

    while (in1_ix < in1->table.row_count && in2_ix < in2->table.row_count) {
        int64_t time1 = in1->times[in1_ix];
        int64_t time2 = in2->times[in2_ix];
        int16_t prio1 = in1->priorities[in1_ix];
        int16_t prio2 = in2->priorities[in2_ix];

        // ordered by time, priority. lowest priority first
        bool64_t copy_from_1 = (time1 < time2)
            || (time1 == time2 && prio1 < prio2);

        if (copy_from_1) {
            err = merge_append_attribute (in1, in1_ix, attr);
            if (err) return err;

            in1_ix++;
        } else {
            err = merge_append_attribute (in2, in2_ix, attr);
            if (err) return err;

            in2_ix++;
        }
    }

    // fixup loops after one of the inputs is finished
    while (in1_ix < in1->table.row_count) {
        err = merge_append_attribute (in1, in1_ix, attr);
        if (err) return err;

        in1_ix++;
    }
    while (in2_ix < in2->table.row_count) {
        err = merge_append_attribute (in2, in2_ix, attr);
        if (err) return err;

        in2_ix++;
    }

    return ZEBRA_SUCCESS;
}

error_t empty_table (anemone_mempool_t *pool, zebra_table_t *in, zebra_table_t *out)
{
    int64_t count = in->column_count;
    out->column_count = count;
    out->columns = anemone_mempool_calloc (pool, count, sizeof(zebra_column_t));
    for (int64_t c = 0; c < count; ++c) {
        zebra_type_t type = in->columns[c].type;
        out->columns[c].type = type;
        if (type == ZEBRA_ARRAY) {
            empty_table (&in->columns[c].data.a.table, &out->columns[c].data.a.table);
        }
    }
    return ZEBRA_SUCCESS;
}

error_t empty_attribute (anemone_mempool_t *pool, zebra_attribute_t *in, zebra_attribute_t **out)
{
    error_t err;

    zebra_attribute_t *attr = anemone_mempool_calloc (pool, 1, sizeof(zebra_attribute_t));

    err = empty_table (&in->table, &attr->table);
    if (err) return err;

    *out = attr;
    return ZEBRA_SUCCESS;
}

