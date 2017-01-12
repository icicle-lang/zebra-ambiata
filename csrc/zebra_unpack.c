#include "zebra_unpack.h"

#if CABAL
#include "anemone_pack.h"
#else
#include "../lib/anemone/csrc/anemone_pack.h"
#endif

ANEMONE_STATIC
ANEMONE_INLINE
int64_t zebra_unzigzag64(uint64_t n)
{
    return (n >> 1) ^ (-(n & 1));
}

ANEMONE_STATIC
ANEMONE_INLINE
uint64_t zebra_zigzag64(int64_t n)
{
    return (n << 1) ^ (n >> 63);
}

ANEMONE_STATIC
ANEMONE_INLINE
int64_t zebra_midpoint (
    int64_t *elems
  , int64_t n_elems )
{
    if (n_elems == 0) return 0;

    int64_t min = elems[0];
    int64_t max = elems[0];
    for (int64_t c = 1; c != n_elems; ++c) {
        int64_t elem = elems[c];
        min = elem < min ? elem : min;
        max = elem > max ? elem : max;
    }
    
    return (min & max) + ((min ^ max) >> 1);
}

error_t zebra_unpack_array (
    uint8_t *buf
  , int64_t bufsize
  , int64_t n_elems
  , int64_t offset
  , int64_t *fill_start
  )
{
    error_t error;

    const int64_t int_part_size = 64;

    int64_t n_parts   = n_elems / int_part_size;
    int64_t n_remains = n_elems % int_part_size;

    int64_t *fill  = fill_start;

    uint8_t *nbits = buf;
    uint8_t *parts = buf + n_parts;
    for (int64_t ix = 0; ix != n_parts; ++ix) {
        uint8_t nbit = *nbits;

        error = anemone_unpack64_64 (1, nbit, parts, (uint64_t*)fill);
        if (error) return error;

        nbits += 1;
        // we have read 64 ints
        fill  += 64;
        // but it took how many bytes...
        parts += nbit * 8;
    }

    int64_t *remains = (int64_t*)parts;
    for (int64_t ix = 0; ix != n_remains; ++ix) {
        *fill = *remains;
        ++fill;
        ++remains;
    }

    for (int64_t ix = 0; ix != n_elems; ++ix) {
        fill_start[ix] = zebra_unzigzag64(fill_start[ix]) + offset;
    }

    return 0;
}

error_t zebra_pack_array (
    uint8_t **buf_inout
  , int64_t *elems
  , int64_t n_elems
  )
{
    error_t error;

    const int64_t int_part_size = 64;

    int64_t n_parts   = n_elems / int_part_size;
    int64_t n_remains = n_elems % int_part_size;


    int64_t offset = zebra_midpoint (elems, n_elems);

    uint8_t *buf = *buf_inout;
    uint8_t *nbits = buf + 8;
    uint8_t *parts = nbits + n_parts;   
    *((uint64_t*)buf) = offset;

    uint64_t deltas[int_part_size];
    
    for (int64_t part_ix = 0; part_ix != n_parts; ++part_ix) {

        uint64_t max_delta = 0;
        for (int64_t delta_ix = 0; delta_ix != int_part_size; ++delta_ix) {
            uint64_t delta = zebra_zigzag64 (elems[delta_ix] - offset);
            deltas[delta_ix] = delta;
            max_delta = delta > max_delta ? delta : max_delta;
        }

        uint64_t bitsof = max_delta ? 64 - __builtin_clzll (max_delta) : 0;
        uint8_t  nbit = (uint8_t)bitsof;

        *nbits = nbit;

        error = anemone_pack64_64 (1, nbit, deltas, parts);
        if (error) return error;

        nbits += 1;
        // we have read 64 ints
        elems += 64;
        // but it took how many bytes...
        parts += nbit * 8;
    }

    int64_t *remains = (int64_t*)parts;
    for (int64_t ix = 0; ix != n_remains; ++ix) {
        *remains = zebra_zigzag64 (*elems - offset);
        ++elems;
        ++remains;
    }

    *buf_inout = (uint8_t*)remains;

    return 0;
}

