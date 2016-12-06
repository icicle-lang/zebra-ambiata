#include "zebra_clone.h"

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
// Neritic clones (in between the shallow and the deep), here we clone table
// but keep the original reference to the data. The clone of the table's
// structure is deep, but the clone of the data is shallow.
//

error_t zebra_neritic_clone_column (
    anemone_mempool_t *pool
  , zebra_column_t *in_column
  , zebra_column_t *out_column )
{
    zebra_type_t type = in_column->type;

    out_column->type = type;

    zebra_data_t *in_data = &in_column->data;
    zebra_data_t *out_data = &out_column->data;

    switch (type) {
        case ZEBRA_BYTE:
            out_data->b = in_data->b;
            return ZEBRA_SUCCESS;

        case ZEBRA_INT:
            out_data->i = in_data->i;
            return ZEBRA_SUCCESS;

        case ZEBRA_DOUBLE:
            out_data->d = in_data->d;
            return ZEBRA_SUCCESS;

        case ZEBRA_ARRAY:
            out_data->a.n = in_data->a.n;
            return zebra_neritic_clone_table (pool, &in_data->a.table, &out_data->a.table);

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
}

error_t zebra_neritic_clone_columns (
    anemone_mempool_t *pool
  , int64_t column_count
  , zebra_column_t *in_columns
  , zebra_column_t **out_columns )
{
    error_t err;

    zebra_column_t *columns = anemone_mempool_calloc (pool, column_count, sizeof (zebra_column_t));

    for (int64_t i = 0; i < column_count; i++) {
        err = zebra_neritic_clone_column (pool, in_columns + i, columns + i);
        if (err) return err;
    }

    *out_columns = columns;

    return ZEBRA_SUCCESS;
}

error_t zebra_neritic_clone_table (
    anemone_mempool_t *pool
  , zebra_table_t *in_table
  , zebra_table_t *out_table )
{
    out_table->row_count = in_table->row_count;
    out_table->row_capacity = 0;

    int64_t column_count = in_table->column_count;
    out_table->column_count = column_count;

    return zebra_neritic_clone_columns (pool, column_count, in_table->columns, &out_table->columns);
}

error_t zebra_neritic_clone_tables (
    anemone_mempool_t *pool
  , int64_t table_count
  , zebra_table_t *in_tables
  , zebra_table_t **out_tables )
{
    error_t err;

    zebra_table_t *tables = anemone_mempool_alloc (pool, table_count * sizeof (zebra_table_t));

    for (int64_t i = 0; i < table_count; i++) {
        err = zebra_neritic_clone_table (pool, in_tables + i, tables + i);
        if (err) return err;
    }

    *out_tables = tables;

    return ZEBRA_SUCCESS;
}


//
// Deep clones
//

void *zebra_clone_array (anemone_mempool_t *pool, const void *in, int64_t num_elements, int64_t element_size)
{
    int64_t bytes = num_elements * element_size;
    void *out = anemone_mempool_alloc (pool, bytes);
    memcpy (out, in, bytes);
    return out;
}

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

error_t zebra_deep_clone_attribute (anemone_mempool_t *pool, const zebra_attribute_t *attribute, zebra_attribute_t *into)
{
    int64_t capacity = attribute->table.row_capacity;
    into->times = zebra_clone_array (pool, attribute->times, capacity, sizeof (attribute->times[0]) );
    into->priorities = zebra_clone_array (pool, attribute->priorities, capacity, sizeof (attribute->priorities[0]) );
    into->tombstones = zebra_clone_array (pool, attribute->tombstones, capacity, sizeof (attribute->tombstones[0]) );
    return zebra_deep_clone_table(pool, &attribute->table, &into->table);
}

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


