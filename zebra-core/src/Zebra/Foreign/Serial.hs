{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Foreign.Serial (
    unpackArray
  , packArray
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Unsafe as ByteString
import qualified Data.Vector.Storable.Mutable as Mutable
import           Data.Word (Word8)

import           Foreign.Marshal.Alloc (alloca)
import           Foreign.Storable (Storable(..))
import           Foreign.Ptr (Ptr)

import           P

import qualified Prelude as Savage

import           System.IO (IO)
import           System.IO.Unsafe (unsafePerformIO)

import qualified X.Data.Vector.Storable as Storable

import           Zebra.Foreign.Bindings
import           Zebra.Foreign.Util


unpackArray :: ByteString -> Int -> Int -> Either ForeignError (Storable.Vector Int64)
unpackArray bytes elems offset =
  unsafePerformIO $ do
    fill <- Mutable.new elems
    err <- ByteString.unsafeUseAsCString bytes $ \buf ->
           Mutable.unsafeWith fill $ \fill' ->
           unsafe'c'zebra_unpack_array buf (i64 $ ByteString.length bytes) (i64 elems) (i64 offset) fill'

    case fromCError err of
     Left err' -> return $ Left err'
     Right ()  -> Right <$> Storable.unsafeFreeze fill

i64 :: Int -> Int64
i64 = fromIntegral

-- | Pack an array into a buffer. Assumes there is enough space for the whole output.
-- This type signature is designed to be used with Builder
packArray :: Storable.Vector Int64 -> Ptr Word8 -> IO (Ptr Word8)
packArray elems buf
 = alloca $ \bufp -> do
    poke bufp buf
    let len = i64 $ Storable.length elems
    -- XXX: throwing away the error
    err <- Storable.unsafeWith elems $ \elemsp -> unsafe'c'zebra_pack_array bufp elemsp len
    case fromCError err of
     Left err' -> Savage.error ("Zebra.Foreign.Serial.packArray: C code returned error " <> show err')
     Right ()  -> peek bufp
