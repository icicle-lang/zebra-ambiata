{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Foreign.Serial (
    unpackArray
  ) where

import           Zebra.Foreign.Bindings
import           Zebra.Foreign.Util

import           System.IO.Unsafe (unsafePerformIO)

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Unsafe as BU

import qualified X.Data.Vector.Storable as Storable
import qualified Data.Vector.Storable.Mutable as Mutable

import           P

unpackArray :: ByteString -> Int -> Int -> Int -> Either ForeignError (Storable.Vector Int64)
unpackArray bytes bufsize elems offset
 = unsafePerformIO $ do
    fill <- Mutable.new elems
    err <- BU.unsafeUseAsCString bytes $ \buf   ->
           Mutable.unsafeWith fill     $ \fill' ->
           unsafe'c'zebra_unpack_array buf (i64 bufsize) (i64 elems) (i64 offset) fill'

    -- something better fix this
    case fromCError err of
     Left err' -> return $ Left err'
     Right ()  -> Right <$> Storable.unsafeFreeze fill
 where
  i64 :: Int -> Int64
  i64 = fromIntegral

