#include "zebra_append.h"
#include "zebra_clone.h"
#include "zebra_data.h"
#include "zebra_grow.h"
#include "zebra_merge.h"


ANEMONE_STATIC
error_t zebra_merge_attributes (anemone_mempool_t *pool, zebra_attribute_t **ins, int64_t ins_count, zebra_attribute_t *out_into)
{
    error_t err;
    // XXX: would it be better to allocate this in zebra_merge_entities and zero out on every call?
    int64_t *in_ixs = calloc(ins_count, sizeof(int64_t));

    err = zebra_agile_clone_attribute (pool, ins[0], out_into);
    if (err) goto clean;

    // zebra_append_attribute can be expensive, since it has to traverse down to find the values.
    // we can get some gains by saving up calls to it, so that if we have inputs something like
    // > [ 1, 2, 3 ]
    // and
    // > [ 4, 5 ]
    // we don't have to call zebra_append_attribute separately for each of 1, 2 and 3.
    // instead we call it to copy all three elements at once.
    int64_t pending_copy_count = 0;
    int64_t pending_copy_from_ix = 0;

    while (1) {
        int64_t alive = 0;
        int64_t min_time;
        int64_t min_fsid;
        int64_t min_ix;

        for (int64_t at_ix = 0; at_ix != ins_count; ++at_ix) {
            zebra_attribute_t *in = ins[at_ix];
            int64_t count = in->table.row_count;
            int64_t in_ix = in_ixs[at_ix];

            if (in_ix < count) {
                int64_t in_time = in->times[in_ix];
                int64_t in_fsid = in->factset_ids[in_ix];

                bool64_t take_this = (alive == 0)
                    || (in_time < min_time)
                    || (in_time == min_time && in_fsid > min_fsid);
                if (take_this) {
                    min_time = in_time;
                    min_fsid = in_fsid;
                    min_ix   = at_ix;
                }

                alive++;
            }
        }

        if (pending_copy_count > 0 && (pending_copy_from_ix != min_ix || alive == 0)) {
            err = zebra_append_attribute (pool, ins[pending_copy_from_ix], in_ixs[pending_copy_from_ix] - pending_copy_count, out_into, pending_copy_count);
            if (err) return err;

            pending_copy_count = 0;
        }

        if (alive == 0) break;

        pending_copy_from_ix = min_ix;
        pending_copy_count++;
        in_ixs[min_ix]++;

    }

    err = ZEBRA_SUCCESS;
clean:
    free (in_ixs);

    return err;
}

error_t zebra_merge_entities (anemone_mempool_t *pool, zebra_entity_t **ins, int64_t ins_count, zebra_entity_t *out_into)
{
    error_t err;

    if (ins_count == 0) return ZEBRA_MERGE_NO_ENTITIES;
    const zebra_entity_t *in1 = ins[0];

    out_into->hash            = in1->hash;
    out_into->id_length       = in1->id_length;
    out_into->id_bytes        = in1->id_bytes;
    out_into->attribute_count = in1->attribute_count;

    out_into->attributes = anemone_mempool_alloc (pool, sizeof (zebra_attribute_t) * out_into->attribute_count );

    zebra_attribute_t **in_ats = malloc(ins_count * sizeof(zebra_attribute_t*));

    for (int64_t attr_ix = 0; attr_ix != out_into->attribute_count; ++attr_ix) {
        // This might be a waste of time, but it simplifies and might give better locality.
        // Might give even better locality if we copied values rather than pointers?
        for (int64_t ent_ix = 0; ent_ix != ins_count; ++ent_ix)
            in_ats[ent_ix] = ins[ent_ix]->attributes + attr_ix;

        err = zebra_merge_attributes (pool, in_ats, ins_count, out_into->attributes + attr_ix);
        if (err) goto clean;
    }

    err = ZEBRA_SUCCESS;
clean:
    free (in_ats);

    return err;
}

error_t zebra_merge_entity_pair (
    anemone_mempool_t *pool
  , zebra_entity_t *in1
  , zebra_entity_t *in2
  , zebra_entity_t *out_into
  )
{
    zebra_entity_t *ins[2] = {in1, in2};
    return zebra_merge_entities (pool, ins, 2, out_into);
}

