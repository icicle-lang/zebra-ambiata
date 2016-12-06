#ifndef __ZEBRA_HASH_H
#define __ZEBRA_HASH_H

#if CABAL
#include "anemone_base.h"
#include "anemone_hash.h"
#else
#include "../lib/anemone/csrc/anemone_base.h"
#include "../lib/anemone/csrc/anemone_hash.h"
#endif

#define ZEBRA_HASH_SEED ((uint64_t) 0xf7a646480e5a3c0f)

ANEMONE_STATIC
ANEMONE_INLINE
uint32_t zebra_hash (const uint8_t *buf, size_t len) {
    return anemone_fasthash32 (ZEBRA_HASH_SEED, buf, len);
}

#endif//__ZEBRA_HASH_H
