#include "zebra_append.h"
#include "zebra_clone.h"
#include "zebra_data.h"
#include "zebra_grow.h"
#include "zebra_merge.h"


error_t zebra_merge_attribute (anemone_mempool_t *pool, const zebra_attribute_t *in1, const zebra_attribute_t *in2, zebra_attribute_t *out_into)
{
    error_t err;

    err = zebra_agile_clone_attribute (pool, in1, out_into);
    if (err) return err;

    int64_t in1_ix = 0;
    int64_t in2_ix = 0;

    // zebra_append_attribute can be expensive, since it has to traverse down to find the values.
    // we can get some gains by saving up calls to it, so that if we have inputs something like
    // > [ 1, 2, 3 ]
    // and
    // > [ 4, 5 ]
    // we don't have to call zebra_append_attribute separately for each of 1, 2 and 3.
    // instead we call it to copy all three elements at once.
    // pending_out1 and pending_out2 are used to keep track of how many values we would have copied.
    //
    // the loop invariant is that at least one of these are zero:
    // pending_out1 == 0 \/ pending_out2 == 0
    int64_t pending_out1 = 0;
    int64_t pending_out2 = 0;

    while (in1_ix < in1->table.row_count && in2_ix < in2->table.row_count) {
        int64_t time1 = in1->times[in1_ix];
        int64_t time2 = in2->times[in2_ix];
        int64_t prio1 = in1->priorities[in1_ix];
        int64_t prio2 = in2->priorities[in2_ix];

        // ordered by time, priority. lowest priority first
        bool64_t copy_from_1 = (time1 < time2)
            || (time1 == time2 && prio1 < prio2);

        if (copy_from_1) {
            in1_ix++;
            pending_out1++;
            // check if we need to write anything from 2 first:
            // if so, flush it and set it to zero.
            if (pending_out2 > 0) {
                err = zebra_append_attribute (pool, in2, in2_ix - pending_out2, out_into, pending_out2);
                if (err) return err;

                pending_out2 = 0;
            }
        } else {
            in2_ix++;
            pending_out2++;
            if (pending_out1 > 0) {
                err = zebra_append_attribute (pool, in1, in1_ix - pending_out1, out_into, pending_out1);
                if (err) return err;

                pending_out1 = 0;
            }
        }
    }

    // flush any pending outputs we need to.
    // we could do this with the fixup loops below, but one extra call shouldn't hurt.
    if (pending_out1 > 0) {
        err = zebra_append_attribute (pool, in1, in1_ix - pending_out1, out_into, pending_out1);
        if (err) return err;
    }
    if (pending_out2 > 0) {
        err = zebra_append_attribute (pool, in2, in2_ix - pending_out2, out_into, pending_out2);
        if (err) return err;
    }

    // assert (in1_ix == in1->table.row_count || in2_ix == in2->table.row_count)
    // fixup loops after one of the inputs is finished
    if (in1_ix < in1->table.row_count) {
        err = zebra_append_attribute (pool, in1, in1_ix, out_into, in1->table.row_count - in1_ix);
        if (err) return err;
    } else if (in2_ix < in2->table.row_count) {
        err = zebra_append_attribute (pool, in2, in2_ix, out_into, in2->table.row_count - in2_ix);
        if (err) return err;
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


