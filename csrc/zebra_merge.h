#ifndef __ZEBRA_MERGE_H
#define __ZEBRA_MERGE_H

#include "zebra_data.h"

error_t merge_attribute (anemone_mempool_t *pool, zebra_attribute_t *in1, zebra_attribute_t *in2, zebra_attribute_t **out);

#endif//__ZEBRA_MERGE_H
