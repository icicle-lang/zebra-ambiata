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
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Build
import           Data.Coerce (coerce)
import qualified Data.List as List

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
    Build.word32LE (fromIntegral $ B.length uncompressed) <>
    Build.word32LE (fromIntegral $ B.length compressed) <>
    Build.byteString compressed

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
--   Word arrays pack sets of 64 integers in to BP64 encoded chunks. If there
--   isn't an exact multiple of 64 then the overflow integers are stored after
--   the chunks and encoded using VByte.
--
-- @
--   int_array n {
--     size    : u32
--     nbits   : (n `div` 64) x u8
--     parts   : map bp64 nbits
--     remains : (n `mod` 64) x vbyte
--   }
-- @
--
bIntArray :: Storable.Vector Int64 -> Builder
bIntArray xs0 =
  let
    -- TODO zig-zag encoding
    -- TODO offset/delta encoding
    xs =
      coerce xs0

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
      Storable.slice ix intPartSize xs

    packs :: [Packed64]
    packs =
      fmap (maybe impossible id . pack) .
      List.take n_parts $
      List.iterate (+ intPartSize) 0

    nbits :: Builder
    nbits =
      foldMap (Build.word8 . fromIntegral . packedBits) packs

    parts :: Builder
    parts =
      foldMap (Build.byteString . packedBytes) packs

    -- TODO should be vint, not u64
    remains :: Builder
    remains =
      foldMap Build.word64LE . Storable.toList $
      Storable.slice (n - n_remains) n_remains xs

    size :: Builder
    size =
      Build.word32LE . fromIntegral $
        length packs +
        intSize * sum (fmap packedBits packs) +
        (8 * n_remains) -- TODO should be vint, not u64
  in
    size <>
    nbits <>
    parts <>
    remains

getIntArray :: Int -> Get (Storable.Vector Int64)
getIntArray n = do
  size <- fromIntegral <$> Get.getWord32le
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

    pure . coerce .
      Storable.concat $ words <> [remains]

intPartSize :: Int
intPartSize =
  64

intSize :: Int
intSize =
  8
