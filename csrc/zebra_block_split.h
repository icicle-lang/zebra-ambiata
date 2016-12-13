#ifndef __ZEBRA_BLOCK_SPLIT_H
#define __ZEBRA_BLOCK_SPLIT_H

#include "zebra_data.h"


error_t zebra_entities_of_block (
    anemone_mempool_t *pool
  , zebra_block_t *block
  , int64_t *out_entity_count
  , zebra_entity_t **out_entities
  );


#endif//__ZEBRA_BLOCK_SPLIT_H

