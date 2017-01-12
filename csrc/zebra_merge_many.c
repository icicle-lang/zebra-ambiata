#include "zebra_clone.h"
#include "zebra_merge_many.h"
#include "zebra_merge.h"


ANEMONE_STATIC
ANEMONE_INLINE
int64_t zebra_compare_strings(const uint8_t* buf1, int64_t len1, const uint8_t* buf2, int64_t len2)
{
    uint64_t min = (len1 < len2) ? len1 : len2;
    int64_t cmp = anemone_memcmp(buf1, buf2, min);

    if (cmp == 0)
        /* if buf1 is shorter, it is smaller */
        return len1 - len2;
    else
        return cmp;
}

ANEMONE_STATIC
ANEMONE_INLINE
int64_t zebra_entity_compare(zebra_entity_t *c1, zebra_entity_t *c2)
{
    int64_t c;
    c = (int64_t)c1->hash - (int64_t)c2->hash;
    if (c != 0) return c;
    return zebra_compare_strings(c1->id_bytes, c1->id_length, c2->id_bytes, c2->id_length);
}

error_t zebra_mm_init (anemone_mempool_t *pool, zebra_merge_many_t **merger)
{
    *merger = anemone_mempool_calloc (pool, 1, sizeof (zebra_merge_many_t) );

    return ZEBRA_SUCCESS;
}

error_t zebra_mm_push (anemone_mempool_t *pool, zebra_merge_many_t *merger, int64_t add_count, zebra_entity_t **add_entities)
{
    int64_t merger_count = merger->count;
    int64_t alloc_count  = merger->count + add_count;

    zebra_entity_t **entities = anemone_mempool_alloc (pool, alloc_count * sizeof(zebra_entity_t*));
    zebra_entity_t **merger_entities = merger->entities;

    int64_t merger_ix = 0;
    int64_t add_ix    = 0;
    int64_t insert_ix = 0;

    while (merger_ix != merger_count && add_ix != add_count) {
        zebra_entity_t *e1 = merger_entities[merger_ix];
        zebra_entity_t *e2 = add_entities[add_ix];

        int64_t cmp = zebra_entity_compare(e1, e2);
        if (cmp <= 0) {
            entities[insert_ix] = e1;
            merger_ix++;
        } else {
            entities[insert_ix] = e2;
            add_ix++;
        }
        insert_ix++;
    }

    // fixup loops
    while (merger_ix != merger_count) {
        entities[insert_ix++] = merger_entities[merger_ix++];
    }
    while (add_ix != add_count) {
        entities[insert_ix++] = add_entities[add_ix++];
    }

    merger->count = insert_ix;
    merger->entities = entities;

    return ZEBRA_SUCCESS;
}

error_t zebra_mm_pop (anemone_mempool_t *pool, zebra_merge_many_t *merger, zebra_entity_t **out)
{
    int64_t count = merger->count;
    if (count == 0) {
        *out = NULL;
        return ZEBRA_SUCCESS;
    }

    zebra_entity_t **entities = merger->entities;

    int64_t take = 1;
    // Count how many entities have the same entity id as the top
    while (take != count && zebra_entity_compare(entities[0], entities[take]) == 0) {
        ++take;
    }

    merger->entities += take;
    merger->count    -= take;

    if (take == 1) {
        *out = *entities;
        return ZEBRA_SUCCESS;
    }

    zebra_entity_t *into = anemone_mempool_alloc (pool, sizeof(zebra_entity_t));
    error_t err = zebra_merge_entities (pool, entities, take, into);
    if (err) return err;

    *out = into;
    return ZEBRA_SUCCESS;
}



error_t zebra_mm_clone (
    anemone_mempool_t *pool
  , zebra_merge_many_t **merger_inout)
{
    zebra_merge_many_t *merger = *merger_inout;

    int64_t max_count = merger->count;

    zebra_entity_t **old_entities = merger->entities;
    zebra_entity_t **new_entities = anemone_mempool_alloc (pool, max_count * sizeof(zebra_entity_t*));
    zebra_entity_t *new_entity_values = anemone_mempool_alloc (pool, max_count * sizeof(zebra_entity_t));
    merger->entities = new_entities;

    int64_t in_ix = 0;
    int64_t out_ix = 0;
    while (in_ix != max_count) {
        int64_t take_ix = in_ix + 1;
        while (take_ix != max_count && zebra_entity_compare(old_entities[in_ix], old_entities[take_ix]) == 0) {
            ++take_ix;
        }

        int64_t take = take_ix - in_ix;
        zebra_entity_t *out_into = new_entity_values + out_ix;

        if (take == 1) {
            error_t err = zebra_deep_clone_entity (pool, old_entities[in_ix], out_into);
            if (err) return err;
        } else {

            error_t err = zebra_merge_entities (pool, old_entities + in_ix, take, out_into);

            out_into->id_bytes = ZEBRA_CLONE_ARRAY (pool, out_into->id_bytes, out_into->id_length );

            if (err) return err;
        }

        new_entities[out_ix] = out_into;

        in_ix += take;
        out_ix += 1;
    }

    merger->count = out_ix;

    *merger_inout = ZEBRA_CLONE_ARRAY (pool, merger, 1);

    return ZEBRA_SUCCESS;
}

