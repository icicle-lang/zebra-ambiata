#include "zebra_data.h"
#include "zebra_grow.h"
#include "zebra_clone.h"

#include <string.h>

static error_t read_int32 (const uint8_t **pp, const uint8_t *pe, int32_t *out)
{
    const uint8_t *p = *pp;

    if (p + sizeof (int32_t) > pe) return ZEBRA_NOT_ENOUGH_BYTES;

    *out = *(int32_t *)p;
    *pp = p + sizeof (int32_t);

    return ZEBRA_SUCCESS;
}

static error_t read_uint8 (const uint8_t **pp, const uint8_t *pe, uint8_t *out)
{
    const uint8_t *p = *pp;

    if (p + sizeof (uint8_t) > pe) return ZEBRA_NOT_ENOUGH_BYTES;

    *out = *(uint8_t *)p;
    *pp = p + sizeof (uint8_t);

    return ZEBRA_SUCCESS;
}

error_t zebra_alloc_table (
    anemone_mempool_t *pool
  , zebra_table_t *table
  , const uint8_t **pp_schema
  , const uint8_t *pe_schema )
{
    error_t err;

    int32_t column_count;
    err = read_int32 (pp_schema, pe_schema, &column_count);
    if (err) return err;

    zebra_column_t *columns = anemone_mempool_calloc (pool, column_count, sizeof (zebra_column_t));

    for (int32_t i = 0; i < column_count; i++) {
        uint8_t type_code;
        err = read_uint8 (pp_schema, pe_schema, &type_code);
        if (err) return err;

        switch (type_code) {
            case 'b':
                columns[i].type = ZEBRA_BYTE;
                break;

            case 'i':
                columns[i].type = ZEBRA_INT;
                break;

            case 'd':
                columns[i].type = ZEBRA_DOUBLE;
                break;

            case 'a':
                columns[i].type = ZEBRA_ARRAY;
                err = zebra_alloc_table (pool, &columns[i].data.a.table, pp_schema, pe_schema);
                if (err) return err;
                break;

            default:
                return ZEBRA_INVALID_COLUMN_TYPE;
        }
    }

    table->row_capacity = 0;
    table->column_count = column_count;
    table->columns = columns;

    return ZEBRA_SUCCESS;
}

error_t zebra_add_row (
    anemone_mempool_t *pool
  , zebra_entity_t *entity
  , int32_t attribute_id
  , int64_t time
  , int64_t factset_id
  , bool64_t tombstone
  , zebra_column_t **out_columns
  , int64_t *out_index )
{
    if (attribute_id < 0 || attribute_id >= entity->attribute_count)
        return ZEBRA_ATTRIBUTE_NOT_FOUND;

    zebra_attribute_t *attribute = entity->attributes + attribute_id;

    error_t err = zebra_grow_attribute (pool, attribute);
    if (err) return err;

    zebra_table_t *table = &attribute->table;
    zebra_column_t *columns = table->columns;
    int64_t count = table->row_count;

    *out_columns = columns;
    *out_index = count;

    int64_t *times = attribute->times;
    int64_t *factset_ids = attribute->factset_ids;
    bool64_t *tombstones = attribute->tombstones;

    times[count] = time;
    factset_ids[count] = factset_id;
    tombstones[count] = tombstone;

    table->row_count = count + 1;

    return ZEBRA_SUCCESS;
}

