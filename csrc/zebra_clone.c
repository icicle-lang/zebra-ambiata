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
    into->factset_ids = NULL;
    into->tombstones = NULL;
    return zebra_agile_clone_table(pool, &attribute->table, &into->table);
}

error_t zebra_agile_clone_table (anemone_mempool_t *pool, const zebra_table_t *table, zebra_table_t *into)
{
    error_t err;

    into->row_count = 0;
    into->row_capacity = 0;
    into->tag = table->tag;
    
    switch (table->tag) {
        case ZEBRA_TABLE_BINARY:
            into->of._binary.bytes = NULL;
            return ZEBRA_SUCCESS;

        case ZEBRA_TABLE_ARRAY:
            return zebra_agile_clone_column (pool, table->of._array.values, into->of._array.values);

        case ZEBRA_TABLE_MAP:
            err = zebra_agile_clone_column (pool, table->of._array.keys, into->of._array.keys);
            if (err) return err;
            return zebra_agile_clone_column (pool, table->of._array.values, into->of._array.values);

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
}

error_t zebra_agile_clone_column (
    anemone_mempool_t *pool
  , const zebra_column_t *in_column
  , zebra_column_t *out_column
  )
{
    error_t err;

    zebra_column_tag_t type = in_column->type;
    out_column->type = type;
    zebra_column_variant_t *in_data = &in_column->of;
    zebra_column_variant_t *out_data = &out_column->of;

    switch (type) {
        case ZEBRA_COLUMN_UNIT:
            return ZEBRA_SUCCESS;

        case ZEBRA_COLUMN_INT:
            out_data->_int.values = NULL;
            return ZEBRA_SUCCESS;

        case ZEBRA_COLUMN_DOUBLE:
            out_data->_double.values = NULL;
            return ZEBRA_SUCCESS;

        case ZEBRA_COLUMN_ENUM: {
            out_data->_enum.tags = NULL;
            int64_t c = in_data->_enum.column_count;
            for (int64_t i = 0; i != c; ++i) {
                err = zebra_agile_clone_column (pool, in_data->_enum.columns[i], out_data->_enum.columns[i]);
                if (err) return err;
            }
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_STRUCT: {
            int64_t c = in_data->_struct.column_count;
            for (int64_t i = 0; i != c; ++i) {
                err = zebra_agile_clone_column (pool, in_data->_struct.columns[i], out_data->_struct.columns[i]);
                if (err) return err;
            }
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_NESTED:
            out_data->_nested.indices = NULL;
            return zebra_agile_clone_table (pool, in_data->_nested.table, out_data->_nested.table);

        default:
            return ZEBRA_INVALID_COLUMN_TYPE;
    }
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
    zebra_column_tag_t type = in_column->type;

    out_column->type = type;

    zebra_column_variant_t *in_data = &in_column->of;
    zebra_column_variant_t *out_data = &out_column->of;

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

    zebra_column_t *columns = anemone_mempool_alloc (pool, column_count * sizeof (zebra_column_t));

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
    if (in) memcpy (out, in, bytes);
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
    into->columns = anemone_mempool_alloc (pool, count * sizeof (zebra_column_t));
    for (int64_t c = 0; c < count; ++c) {
        zebra_column_variant_t *table_data = &table->columns[c].of;
        zebra_column_variant_t *into_data = &into->columns[c].of;

        zebra_column_tag_t type = table->columns[c].type;
        into->columns[c].type = type;

        switch (type) {
            case ZEBRA_BYTE:
                into_data->b = ZEBRA_CLONE_ARRAY (pool, table_data->b, row_capacity );
                break;
            case ZEBRA_INT:
                into_data->i = ZEBRA_CLONE_ARRAY (pool, table_data->i, row_capacity );
                break;
            case ZEBRA_DOUBLE:
                into_data->d = ZEBRA_CLONE_ARRAY (pool, table_data->d, row_capacity );
                break;
            case ZEBRA_ARRAY:
                into_data->a.n = ZEBRA_CLONE_ARRAY (pool, table_data->a.n, row_capacity + 1);
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
    into->times = ZEBRA_CLONE_ARRAY (pool, attribute->times, capacity );
    into->factset_ids = ZEBRA_CLONE_ARRAY (pool, attribute->factset_ids, capacity );
    into->tombstones = ZEBRA_CLONE_ARRAY (pool, attribute->tombstones, capacity );
    return zebra_deep_clone_table(pool, &attribute->table, &into->table);
}

error_t zebra_deep_clone_entity (anemone_mempool_t *pool, const zebra_entity_t *entity, zebra_entity_t *into)
{
    error_t err;

    into->hash            = entity->hash;
    into->id_length       = entity->id_length;
    into->id_bytes        = ZEBRA_CLONE_ARRAY (pool, entity->id_bytes, entity->id_length );
    into->attribute_count = entity->attribute_count;

    into->attributes = anemone_mempool_alloc (pool, sizeof (zebra_attribute_t) * into->attribute_count );
    for (int64_t c = 0; c < into->attribute_count; ++c) {
        err = zebra_deep_clone_attribute (pool, entity->attributes + c, into->attributes + c);
        if (err) return err;
    }

    return ZEBRA_SUCCESS;
}


