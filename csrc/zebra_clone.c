#include "zebra_clone.h"
#include "zebra_grow.h"


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
    into->row_count = 0;
    into->row_capacity = 0;
    into->tag = table->tag;
    
    switch (table->tag) {
        case ZEBRA_TABLE_BINARY: {
            into->of._binary.bytes = NULL;
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_TABLE_ARRAY: {
            into->of._array.values = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );
            return zebra_agile_clone_column (pool, table->of._array.values, into->of._array.values);
        }

        case ZEBRA_TABLE_MAP: {
            into->of._map.keys = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );
            into->of._map.values = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );

            error_t err = zebra_agile_clone_column (pool, table->of._map.keys, into->of._map.keys);
            if (err) return err;
            return zebra_agile_clone_column (pool, table->of._map.values, into->of._map.values);
        }

        default: {
            return ZEBRA_INVALID_COLUMN_TYPE;
        }
    }
}

error_t zebra_agile_clone_column (
    anemone_mempool_t *pool
  , const zebra_column_t *in_column
  , zebra_column_t *out_column
  )
{
    error_t err;

    zebra_column_tag_t tag = in_column->tag;
    out_column->tag = tag;
    const zebra_column_variant_t *in_data = &in_column->of;
    zebra_column_variant_t *out_data = &out_column->of;

    switch (tag) {
        case ZEBRA_COLUMN_UNIT: {
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_INT: {
            out_data->_int.values = NULL;
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_DOUBLE: {
            out_data->_double.values = NULL;
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_ENUM: {
            out_data->_enum.tags = NULL;
            int64_t c = in_data->_enum.column_count;
            out_data->_enum.columns = anemone_mempool_alloc (pool, c * sizeof (zebra_column_t) );

            for (int64_t i = 0; i != c; ++i) {
                err = zebra_agile_clone_column (pool, in_data->_enum.columns + i, out_data->_enum.columns + i);
                if (err) return err;
            }
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_STRUCT: {
            int64_t c = in_data->_struct.column_count;
            out_data->_struct.columns = anemone_mempool_alloc (pool, c * sizeof (zebra_column_t) );

            for (int64_t i = 0; i != c; ++i) {
                err = zebra_agile_clone_column (pool, in_data->_struct.columns + i, out_data->_struct.columns + i);
                if (err) return err;
            }
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_NESTED: {
            out_data->_nested.indices = NULL;
            return zebra_agile_clone_table (pool, &in_data->_nested.table, &out_data->_nested.table);
        }

        default: {
            return ZEBRA_INVALID_COLUMN_TYPE;
        }
    }
}


//
// Neritic clones (in between the shallow and the deep), here we clone table
// but keep the original reference to the data. The clone of the table's
// structure is deep, but the clone of the data is shallow.
//

error_t zebra_neritic_clone_table (
    anemone_mempool_t *pool
  , zebra_table_t *in_table
  , zebra_table_t *out_table )
{
    out_table->row_count = in_table->row_count;
    // See Note: zebra_table.row_capacity
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Append is more or less the only thing we do that mutates table data.
    // We want to ensure that if the cloned table has values appended, it does not affect the original one.
    // Setting the row_capacity to row_count ensures this because when append calls "grow", it decides it has to copy.
    out_table->row_capacity = in_table->row_count;

    zebra_table_tag_t tag = in_table->tag;
    out_table->tag = tag;
    switch (tag) {
        case ZEBRA_TABLE_BINARY: {
            out_table->of._binary.bytes = in_table->of._binary.bytes;
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_TABLE_ARRAY: {
            out_table->of._array.values = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );
            return zebra_neritic_clone_column (pool, in_table->of._array.values, out_table->of._array.values); 
        }

        case ZEBRA_TABLE_MAP: {
            out_table->of._map.keys = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );
            out_table->of._map.values = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );

            error_t err = zebra_neritic_clone_column (pool, in_table->of._map.keys, out_table->of._map.keys); 
            if (err) return err;
            return zebra_neritic_clone_column (pool, in_table->of._map.values, out_table->of._map.values); 
        }

        default: {
            return ZEBRA_INVALID_COLUMN_TYPE;
        }
    }
}

error_t zebra_neritic_clone_column (
    anemone_mempool_t *pool
  , zebra_column_t *in_column
  , zebra_column_t *out_column )
{
    error_t err;

    zebra_column_tag_t tag = in_column->tag;
    out_column->tag = tag;
    zebra_column_variant_t *in_data = &in_column->of;
    zebra_column_variant_t *out_data = &out_column->of;

    switch (tag) {
        case ZEBRA_COLUMN_UNIT: {
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_INT: {
            out_data->_int.values = in_data->_int.values;
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_DOUBLE: {
            out_data->_double.values = in_data->_double.values;
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_ENUM: {
            out_data->_enum.tags = in_data->_enum.tags;
            int64_t c = in_data->_enum.column_count;
            out_data->_enum.columns = anemone_mempool_alloc (pool, c * sizeof (zebra_column_t) );

            for (int64_t i = 0; i != c; ++i) {
                err = zebra_neritic_clone_column (pool, in_data->_enum.columns + i, out_data->_enum.columns + i);
                if (err) return err;
            }
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_STRUCT: {
            int64_t c = in_data->_struct.column_count;
            out_data->_struct.columns = anemone_mempool_alloc (pool, c * sizeof (zebra_column_t) );

            for (int64_t i = 0; i != c; ++i) {
                err = zebra_neritic_clone_column (pool, in_data->_struct.columns + i, out_data->_struct.columns + i);
                if (err) return err;
            }
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_NESTED: {
            out_data->_nested.indices = in_data->_nested.indices;
            return zebra_neritic_clone_table (pool, &in_data->_nested.table, &out_data->_nested.table);
        }

        default: {
            return ZEBRA_INVALID_COLUMN_TYPE;
        }
    }
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


error_t zebra_deep_clone_table (anemone_mempool_t *pool, const zebra_table_t *in_table, zebra_table_t *out_table)
{
    int64_t row_count = in_table->row_count;
    int64_t row_capacity = in_table->row_capacity;
    zebra_table_tag_t tag = in_table->tag;

    out_table->row_count = row_count;
    out_table->row_capacity = row_capacity;
    out_table->tag = tag;

    switch (tag) {
        case ZEBRA_TABLE_BINARY: {
            out_table->of._binary.bytes = ZEBRA_CLONE_ARRAY (pool, in_table->of._binary.bytes, row_capacity);
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_TABLE_ARRAY: {
            out_table->of._array.values = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );
            return zebra_deep_clone_column (pool, row_capacity, in_table->of._array.values, out_table->of._array.values); 
        }

        case ZEBRA_TABLE_MAP: {
            out_table->of._map.keys = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );
            out_table->of._map.values = anemone_mempool_alloc (pool, sizeof (zebra_column_t) );

            error_t err = zebra_deep_clone_column (pool, row_capacity, in_table->of._map.keys, out_table->of._map.keys); 
            if (err) return err;
            return zebra_deep_clone_column (pool, row_capacity, in_table->of._map.values, out_table->of._map.values); 
        }

        default: {
            return ZEBRA_INVALID_COLUMN_TYPE;
        }
    }
}

error_t zebra_deep_clone_column (
    anemone_mempool_t *pool
  , int64_t capacity
  , const zebra_column_t *in_column
  , zebra_column_t *out_column
  )
{
    error_t err;

    zebra_column_tag_t tag = in_column->tag;
    out_column->tag = tag;
    const zebra_column_variant_t *in_data = &in_column->of;
    zebra_column_variant_t *out_data = &out_column->of;

    switch (tag) {
        case ZEBRA_COLUMN_UNIT: {
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_INT: {
            out_data->_int.values = ZEBRA_CLONE_ARRAY (pool, in_data->_int.values, capacity);
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_DOUBLE: {
            out_data->_double.values = ZEBRA_CLONE_ARRAY (pool, in_data->_double.values, capacity);
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_ENUM: {
            out_data->_enum.tags = ZEBRA_CLONE_ARRAY (pool, in_data->_enum.tags, capacity);
            int64_t c = in_data->_enum.column_count;
            out_data->_enum.columns = anemone_mempool_alloc (pool, c * sizeof (zebra_column_t) );

            for (int64_t i = 0; i != c; ++i) {
                err = zebra_deep_clone_column (pool, capacity, in_data->_enum.columns + i, out_data->_enum.columns + i);
                if (err) return err;
            }
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_STRUCT: {
            int64_t c = in_data->_struct.column_count;
            out_data->_struct.columns = anemone_mempool_alloc (pool, c * sizeof (zebra_column_t) );

            for (int64_t i = 0; i != c; ++i) {
                err = zebra_deep_clone_column (pool, capacity, in_data->_struct.columns + i, out_data->_struct.columns + i);
                if (err) return err;
            }
            return ZEBRA_SUCCESS;
        }

        case ZEBRA_COLUMN_NESTED: {
            out_data->_nested.indices = ZEBRA_CLONE_ARRAY (pool, in_data->_nested.indices, capacity);
            return zebra_deep_clone_table (pool, &in_data->_nested.table, &out_data->_nested.table);
        }

        default: {
            return ZEBRA_INVALID_COLUMN_TYPE;
        }
    }
}


error_t zebra_deep_clone_attribute (anemone_mempool_t *pool, const zebra_attribute_t *attribute, zebra_attribute_t *into)
{
    int64_t capacity = zebra_grow_array_capacity (attribute->table.row_count);
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


