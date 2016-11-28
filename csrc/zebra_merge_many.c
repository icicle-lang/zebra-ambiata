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
    c = c1->hash - c2->hash;
    if (c != 0) return c;
    return zebra_compare_strings(c1->id_bytes, c1->id_length, c2->id_bytes, c2->id_length);
}

error_t zebra_mm_init (anemone_mempool_t *pool, zebra_merge_many_t **merger)
{
    *merger = anemone_mempool_calloc (pool, 1, sizeof (zebra_merge_many_t) );

    return ZEBRA_SUCCESS;
}

error_t zebra_mm_push (anemone_mempool_t *pool, zebra_merge_many_t *merger, int64_t add_count, zebra_entity_t *add_entities)
{
    int64_t merger_count = merger->count;
    int64_t alloc_count  = merger->count + add_count;

    zebra_entity_t *entities = anemone_mempool_calloc (pool, alloc_count, sizeof(zebra_entity_t));

    zebra_entity_t *merger_entities = merger->entities;
    int64_t merger_ix = 0;
    int64_t add_ix    = 0;
    int64_t insert_ix = 0;

    while (merger_ix != merger_count && add_ix != add_count) {
        zebra_entity_t *e1 = merger_entities + merger_ix;
        zebra_entity_t *e2 = add_entities + add_ix;
        zebra_entity_t *into = entities + insert_ix;

        int64_t cmp = zebra_entity_compare(e1, e2);
        if (cmp == 0) {
            error_t err = zebra_merge_entity(pool, e1, e2, into);
            if (err) return err;
            merger_ix++;
            add_ix++;
        } else if (cmp < 0) {
            *into = *e1;
            merger_ix++;
        } else {
            *into = *e2;
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

error_t zebra_mm_pop (zebra_merge_many_t *merger, zebra_entity_t **out)
{
    if (merger->count == 0) {
        *out = NULL;
        return ZEBRA_SUCCESS;
    }

    *out = merger->entities;
    merger->entities++;
    merger->count--;

    return ZEBRA_SUCCESS;
}


ANEMONE_STATIC
void *zebra_clone_array (anemone_mempool_t *pool, const void *in, int64_t num_elements, int64_t element_size)
{
    int64_t bytes = num_elements * element_size;
    void *out = anemone_mempool_alloc (pool, bytes);
    memcpy (out, in, bytes);
    return out;
}

ANEMONE_STATIC
error_t zebra_deep_clone_table (anemone_mempool_t *pool, const zebra_table_t *table, zebra_table_t *into)
{
    error_t err;

    int64_t row_count = table->row_count;
    int64_t row_capacity = table->row_capacity;
    into->row_count = row_count;
    into->row_capacity = row_capacity;

    int64_t count = table->column_count;
    into->column_count = count;
    into->columns = anemone_mempool_calloc (pool, count, sizeof (zebra_column_t));
    for (int64_t c = 0; c < count; ++c) {
        zebra_data_t *table_data = &table->columns[c].data;
        zebra_data_t *into_data = &into->columns[c].data;

        zebra_type_t type = table->columns[c].type;
        into->columns[c].type = type;

        switch (type) {
            case ZEBRA_BYTE:
                into_data->b = zebra_clone_array (pool, table_data->b, row_capacity, sizeof (table_data->b[0]) );
                break;
            case ZEBRA_INT:
                into_data->i = zebra_clone_array (pool, table_data->i, row_capacity, sizeof (table_data->i[0]) );
                break;
            case ZEBRA_DOUBLE:
                into_data->d = zebra_clone_array (pool, table_data->d, row_capacity, sizeof (table_data->d[0]) );
                break;
            case ZEBRA_ARRAY:
                err = zebra_deep_clone_table (pool, &table_data->a.table, &into_data->a.table);
                if (err) return err;
                break;

            default:
                return ZEBRA_INVALID_COLUMN_TYPE;

        }
    }

    return ZEBRA_SUCCESS;
}

ANEMONE_STATIC
error_t zebra_deep_clone_attribute (anemone_mempool_t *pool, const zebra_attribute_t *attribute, zebra_attribute_t *into)
{
    int64_t capacity = attribute->table.row_capacity;
    into->times = zebra_clone_array (pool, attribute->times, capacity, sizeof (attribute->times[0]) );
    into->priorities = zebra_clone_array (pool, attribute->priorities, capacity, sizeof (attribute->priorities[0]) );
    into->tombstones = zebra_clone_array (pool, attribute->tombstones, capacity, sizeof (attribute->tombstones[0]) );
    return zebra_deep_clone_table(pool, &attribute->table, &into->table);
}

ANEMONE_STATIC
error_t zebra_deep_clone_entity (anemone_mempool_t *pool, const zebra_entity_t *entity, zebra_entity_t *into)
{
    error_t err;

    into->hash            = entity->hash;
    into->id_length       = entity->id_length;
    into->id_bytes        = zebra_clone_array (pool, entity->id_bytes, entity->id_length, sizeof (entity->id_bytes[0]) );
    into->attribute_count = entity->attribute_count;

    into->attributes = anemone_mempool_alloc (pool, sizeof (zebra_attribute_t) * into->attribute_count );
    for (int64_t c = 0; c < into->attribute_count; ++c) {
        err = zebra_deep_clone_attribute (pool, entity->attributes + c, into->attributes + c);
        if (err) return err;
    }

    return ZEBRA_SUCCESS;
}



error_t zebra_mm_clone (
    anemone_mempool_t *pool
  , zebra_merge_many_t **merger_inout)
{
    zebra_merge_many_t *merger = *merger_inout;

    int64_t merger_count = merger->count;
    zebra_entity_t *old_entities = merger->entities;
    zebra_entity_t *new_entities = anemone_mempool_calloc (pool, merger_count, sizeof(zebra_entity_t));

    for (int64_t i = 0; i != merger_count; ++i) {
        error_t err = zebra_deep_clone_entity (pool, old_entities + i, new_entities + i);
        if (err) return err;
    }

    *merger_inout = zebra_clone_array (pool, merger, 1, sizeof (zebra_merge_many_t));

    return ZEBRA_SUCCESS;
}

