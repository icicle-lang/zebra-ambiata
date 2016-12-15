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


/* read |blocks| number of blocks of 64 values from |in| at |bits| bits per value, and write 64-bit values to |out|. */
// error_t anemone_unpack64_64 (uint64_t blocks, const uint64_t bits, const uint8_t *in, uint64_t *out) {

/*

    let
      (n_parts, n_remains) =
        elems `quotRem` intPartSize

      unpack nbits bs =
        case Anemone.unpack64 $ Packed64 1 nbits bs of
          Nothing ->
            fail $ "Could not unpack 64 x " <> show nbits <> "-bit words"
          Just xs ->
            pure xs

    nbits <- replicateM n_parts $ fmap fromIntegral Get.getWord8
    parts <- traverse (Get.getByteString . (* intSize)) nbits
    remains <- Storable.replicateM n_remains Get.getWord64le
    words <- zipWithM unpack nbits parts

    pure .
      Storable.map ((+ offset) . unZigZag64) .
      Storable.concat $ words <> [remains]

unZigZag64 :: Word64 -> Int64
unZigZag64 !n =
  fromIntegral $! (n `shiftR` 1) `xor` negate (n .&. 0x1)
*/

