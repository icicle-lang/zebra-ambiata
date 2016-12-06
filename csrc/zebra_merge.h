#ifndef __ZEBRA_MERGE_H
#define __ZEBRA_MERGE_H

#include "zebra_data.h"

error_t zebra_merge_attribute (
    anemone_mempool_t *pool
  , const zebra_attribute_t *in1
  , const zebra_attribute_t *in2
  , zebra_attribute_t *out_into
  );

error_t zebra_merge_entity (
    anemone_mempool_t *pool
  , const zebra_entity_t *in1
  , const zebra_entity_t *in2
  , zebra_entity_t *out_into
  );

#endif//__ZEBRA_MERGE_H
