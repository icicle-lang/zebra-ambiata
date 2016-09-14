{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Serial.Array (
    bStrings
  , bByteArray
  , bIntArray

  , getStrings
  , getByteArray
  , getIntArray
  ) where

import           Anemone.Foreign.Pack (Packed64(..))
import qualified Anemone.Foreign.Pack as Anemone

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.Bits ((.&.), xor, shiftR, shiftL)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.List as List
import           Data.Word (Word64)

import           P

import qualified Prelude as Savage

import qualified Snapper as Snappy

import           X.Data.ByteString.Unsafe (unsafeSplits)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Storable as Storable


-- | Strings are encoded as a word array of lengths, followed by the bytes for
--   each string concatentated.
bStrings :: Boxed.Vector ByteString -> Builder
bStrings bss =
  let
    lengths =
      bIntArray .
      Storable.convert $
      fmap (fromIntegral . B.length) bss

    bytes =
      bByteArray .
      B.concat $
      toList bss
  in
    lengths <>
    bytes

getStrings :: Int -> Get (Boxed.Vector ByteString)
getStrings n = do
  lengths <- getIntArray n
  bytes <- getByteArray
  pure .
    unsafeSplits id bytes $
    Storable.map fromIntegral lengths

-- | Encode a vector of bytes.
--
--   Byte arrays are expected to contain string data and are compressed using
--   something like LZ4 or Snappy.
--
-- @
--   byte_array {
--     size_uncompressed : u32
--     size_compressed   : u32
--     bytes             : size_compressed * u8
--   }
-- @
bByteArray :: ByteString -> Builder
bByteArray uncompressed =
  let
    compressed =
      Snappy.compress uncompressed
  in
    Builder.word32LE (fromIntegral $ B.length uncompressed) <>
    Builder.word32LE (fromIntegral $ B.length compressed) <>
    Builder.byteString compressed

getByteArray :: Get ByteString
getByteArray = do
  n_uncompressed <- Get.getWord32le
  n_compressed <- Get.getWord32le
  compressed <- Get.getByteString $ fromIntegral n_compressed
  case Snappy.decompress compressed of
    Nothing ->
      fail $
        "could not decompress snappy encoded payload " <>
        "(compressed size = " <> show n_compressed <> " bytes" <>
        ", uncompressed size = " <> show n_uncompressed <> " bytes)"
    Just uncompressed ->
      pure uncompressed

-- | Encodes a vector of words.
--
--   Int arrays pack sets of 64 integers in to BP64 encoded chunks. Prior to
--   packing, we find a suitable offset value (e.g. min + (max - min) / 2) and
--   deduct it from each integer to make it closer to zero, we also zig-zag
--   encode each integer so negative numbers don't required 64-bits to store.
--
--   If there isn't an exact multiple of 64 then the overflow integers are
--   stored after the chunks and encoded using VByte.
--
-- @
--   int_array n {
--     size    : u32
--     offset  : i64
--     nbits   : (n `div` 64) x u8
--     parts   : map bp64 nbits
--     remains : (n `mod` 64) x vbyte
--   }
-- @
--
bIntArray :: Storable.Vector Int64 -> Builder
bIntArray xs =
  let
    offset =
      if Storable.null xs then
        0
      else
        midpoint xs

    deltas =
      Storable.map (zigZag64 . subtract offset) xs

    n =
      Storable.length xs

    (n_parts, n_remains) =
      n `quotRem` intPartSize

    impossible :: Packed64
    impossible =
      Savage.error $
        "Blizzard.Zebra.Header.bIntArray: " <>
        "internal error, can only pack multiples " <>
        "of 64, tried to pack <" <> show n <> ">"

    pack :: Int -> Maybe Packed64
    pack ix =
      Anemone.pack64 $
      Storable.slice ix intPartSize deltas

    packs :: [Packed64]
    packs =
      fmap (maybe impossible id . pack) .
      List.take n_parts $
      List.iterate (+ intPartSize) 0

    nbits :: Builder
    nbits =
      foldMap (Builder.word8 . fromIntegral . packedBits) packs

    parts :: Builder
    parts =
      foldMap (Builder.byteString . packedBytes) packs

    -- TODO should be vbyte, not u64
    remains :: Builder
    remains =
      foldMap Builder.word64LE . Storable.toList $
      Storable.slice (n - n_remains) n_remains deltas

    size :: Builder
    size =
      Builder.word32LE . fromIntegral $
        length packs +
        intSize * sum (fmap packedBits packs) +
        (8 * n_remains) -- TODO should be vbyte, not u64
  in
    size <>
    Builder.word64LE (fromIntegral offset) <>
    nbits <>
    parts <>
    remains

getIntArray :: Int -> Get (Storable.Vector Int64)
getIntArray n = do
  size <- fromIntegral <$> Get.getWord32le
  offset <- fromIntegral <$> Get.getWord64le
  Get.isolate size $ do
    let
      (n_parts, n_remains) =
        n `quotRem` intPartSize

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

intPartSize :: Int
intPartSize =
  64

intSize :: Int
intSize =
  8

-- | Find the midpoint between the minimum and maximum.
midpoint :: Storable.Vector Int64 -> Int64
midpoint xs =
  let
    loop (lo, hi) x =
      (min lo x, max hi x)
  in
    uncurry mid64 $
    Storable.foldl' loop (maxBound, minBound) xs
{-# INLINE midpoint #-}

-- | Commutative, overflow proof integer average/midpoint:
--
--   Source: http://stackoverflow.com/a/4844672
--
mid64 :: Int64 -> Int64 -> Int64
mid64 x y =
  (x .&. y) + ((x `xor` y) `shiftR` 1)
{-# INLINE mid64 #-}

zigZag64 :: Int64 -> Word64
zigZag64 !n =
  fromIntegral $! (n `shiftL` 1) `xor` (n `shiftR` 63)
{-# INLINE zigZag64 #-}

unZigZag64 :: Word64 -> Int64
unZigZag64 !n =
  fromIntegral $! (n `shiftR` 1) `xor` negate (n .&. 0x1)
{-# INLINE unZigZag64 #-}
