{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Serial.Binary.Array (
    bStrings
  , bByteArray
  , bSizedByteArray
  , bIntArray

  , getStrings
  , getByteArray
  , getSizedByteArray
  , getIntArray

  -- ** Exported for tests
  , mid64
  , zigZag64
  , unZigZag64
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.Bits ((.&.), xor, shiftR, shiftL)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Builder.Prim.Internal as Prim
import qualified Data.ByteString.Builder.Prim as Prim
import           Data.Word (Word64)

import           P

import qualified Snapper as Snappy

import           X.Data.ByteString.Unsafe (unsafeSplits)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Storable as Storable

import qualified Zebra.Foreign.Serial as Foreign


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
      bSizedByteArray .
      B.concat $
      toList bss
  in
    lengths <>
    bytes
{-# INLINABLE bStrings #-}

getStrings :: Int -> Get (Boxed.Vector ByteString)
getStrings n = do
  lengths <- getIntArray n
  bytes <- getSizedByteArray
  pure .
    unsafeSplits id bytes $
    Storable.map fromIntegral lengths
{-# INLINABLE getStrings #-}

-- | Encode a vector of bytes.
--
--   Byte arrays are expected to contain string data and are compressed using
--   something like LZ4 or Snappy.
--
-- @
--   byte_array n {
--     size_compressed : u32
--     bytes           : size_compressed * u8
--   }
-- @
bByteArray :: ByteString -> Builder
bByteArray uncompressed =
  let
    compressed =
      Snappy.compress uncompressed
  in
    Builder.word32LE (fromIntegral $ B.length compressed) <>
    Builder.byteString compressed
{-# INLINABLE bByteArray #-}

getByteArray :: Int -> Get ByteString
getByteArray n_expected = do
  n_compressed <- Get.getWord32le
  compressed <- Get.getByteString $ fromIntegral n_compressed
  case Snappy.decompress compressed of
    Nothing ->
      fail $
        "could not decompress snappy encoded payload " <>
        "(compressed size = " <> show n_compressed <> " bytes" <>
        ", uncompressed size = " <> show n_expected <> " bytes)"
    Just uncompressed ->
      let
        n_actual =
          B.length uncompressed
      in
        if n_expected == n_actual then
          pure uncompressed
        else
          fail $
            "decoded snappy was the wrong size: " <>
            "expected <" <> show n_expected <>
            ">, but was <" <> show n_actual <> ">"
{-# INLINABLE getByteArray #-}

-- | Encode a vector of bytes.
--
--   Byte arrays are expected to contain string data and are compressed using
--   something like LZ4 or Snappy.
--
-- @
--   sized_byte_array {
--     sized_uncompressed : u32
--     size_compressed    : u32
--     bytes              : size_compressed * u8
--   }
-- @
bSizedByteArray :: ByteString -> Builder
bSizedByteArray uncompressed =
  let
    n =
      B.length uncompressed
  in
    Builder.word32LE (fromIntegral n) <>
    bByteArray uncompressed
{-# INLINABLE bSizedByteArray #-}

getSizedByteArray :: Get ByteString
getSizedByteArray = do
  n_uncompressed <- fromIntegral <$> Get.getWord32le
  getByteArray n_uncompressed
{-# INLINABLE getSizedByteArray #-}

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
  let len = Storable.length xs
      -- Worst case for size of array if packing requires full 64-bit numbers:
      -- size (4)
      -- offset (8)
      -- num-bits (len * 1 byte)
      -- packs worst case (len * 8 byte)
      ensure = 4 + 8 + len + len * 8
      prim = Prim.boudedPrim ensure Foreign.packArray
  in Prim.primBounded prim xs
{-# INLINABLE bIntArray #-}

getIntArray :: Int -> Get (Storable.Vector Int64)
getIntArray elems = do
  bufsize <- fromIntegral <$> Get.getWord32le
  offset <- fromIntegral <$> Get.getWord64le
  bytes <- Get.getByteString bufsize
  case Foreign.unpackArray bytes elems offset of
    Left err -> fail $ "Could not unpack 64-encoded words: " <> show err
    Right xs -> pure xs
{-# INLINABLE getIntArray #-}

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
