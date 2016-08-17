{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Util (
    trippingSerial
  , runGetEither
  , runGetEitherConsumeAll
  ) where

import           Data.Binary.Get (Get, ByteOffset)
import qualified Data.Binary.Get as Get
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Build
import qualified Data.ByteString.Lazy as Lazy
import           Data.String (String)

import           Disorder.Jack (Property, tripping)

import           P


trippingSerial :: (Eq a, Show a) => (a -> Builder) -> Get a -> a -> Property
trippingSerial build get =
  tripping (Build.toLazyByteString . build) (runGetEither get)

runGetEither :: Get a -> Lazy.ByteString -> Either (Lazy.ByteString, ByteOffset, String) a
runGetEither g =
  let
    third (_, _, x) = x
  in
    second third . Get.runGetOrFail g

runGetEitherConsumeAll :: Get a -> Lazy.ByteString -> Either (Lazy.ByteString, ByteOffset, String) a
runGetEitherConsumeAll g bs =
  case Get.runGetOrFail g bs of
    Left err -> Left err
    Right (leftovers,off,v)
     | Lazy.null leftovers
     -> Right v
     | otherwise
     -> Left (leftovers, off, "Not all input consumed")
