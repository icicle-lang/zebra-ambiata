{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.X.Vector.Storable (
    unsafeFromByteString
  , unsafeToByteString
  , module Storable
  ) where

import           Data.ByteString.Internal (ByteString(..))
import           Data.Word (Word8)

import           X.Data.Vector.Storable as Storable


unsafeFromByteString :: ByteString -> Storable.Vector Word8
unsafeFromByteString (PS fp off len) =
  Storable.unsafeFromForeignPtr fp off len

unsafeToByteString :: Storable.Vector Word8 -> ByteString
unsafeToByteString xs =
  let
    (fp, off, len) =
      Storable.unsafeToForeignPtr xs
  in
    PS fp off len
