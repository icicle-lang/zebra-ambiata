#include "zebra_data.h"
#include <stdio.h>

ANEMONE_STATIC
void zebra_debug_print_indent (int64_t indent)
{
    for (int64_t i = 0; i != indent; ++i) printf(" ");
}
#define INDENTF zebra_debug_print_indent(indent); printf


ANEMONE_STATIC
void zebra_debug_print_table (int64_t indent, zebra_table_t *table) ;

ANEMONE_STATIC
void zebra_debug_print_column (int64_t indent, zebra_column_t *column, int64_t row_count)
{
    switch (column->type) {
        case ZEBRA_BYTE:
            INDENTF("BYTE:\n");
            INDENTF("");
            for (int64_t i = 0; i != row_count; ++i) {
                unsigned char c = column->data.b[i];
                if (c > 32 && c < 127)
                    printf("'%c' ", c);
                else
                    printf("(%d) ", c);
            }
            printf("\n");
            return;

        case ZEBRA_INT:
            INDENTF("INT:\n");
            INDENTF("");
            for (int64_t i = 0; i != row_count; ++i) {
                printf("%lld ", column->data.i[i]);
            }
            printf("\n");
            return;

        case ZEBRA_DOUBLE:
            INDENTF("DOUBLE:\n");
            INDENTF("");
            for (int64_t i = 0; i != row_count; ++i) {
                printf("%f ", column->data.d[i]);
            }
            printf("\n");
            return;

        case ZEBRA_ARRAY:
            INDENTF("ARRAY:\n");
            INDENTF(" SEGD:\n");
            INDENTF("  ");
            for (int64_t i = 0; i != row_count; ++i) {
                printf("%lld ", column->data.a.n[i]);
            }
            printf("\n");
            INDENTF(" NESTED:\n");
            zebra_debug_print_table(indent + 2, &column->data.a.table);
            return;

        default:
            INDENTF("INVALID COLUMN\n");
            return;
    }
}

ANEMONE_STATIC
void zebra_debug_print_table (int64_t indent, zebra_table_t *table)
{
    for (int64_t i = 0; i != table->column_count; ++i) {
        INDENTF("Column %lld\n", i);
        zebra_debug_print_column(indent + 1, table->columns + i, table->row_count);
    }
}

ANEMONE_STATIC
void zebra_debug_print_block (zebra_block_t *block)
{
    printf("Block: %lld tables\n", block->table_count);
    for (int64_t i = 0; i != block->table_count; ++i) {
        printf(" Table %lld\n", i);
        zebra_debug_print_table (2, block->tables + i);
    }
}
