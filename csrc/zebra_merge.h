#ifndef __ZEBRA_MERGE_H
#define __ZEBRA_MERGE_H

#include "zebra_data.h"

error_t zebra_merge_entities (
    anemone_mempool_t *pool
  , zebra_entity_t **ins
  , int64_t ins_count
  , zebra_entity_t *out_into
  );

#endif//__ZEBRA_MERGE_H
