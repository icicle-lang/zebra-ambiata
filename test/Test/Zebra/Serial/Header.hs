{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Header where

import           Data.Binary.Get (Get, ByteOffset)
import qualified Data.Binary.Get as Get
import qualified Data.ByteString.Builder as Build
import qualified Data.ByteString.Lazy as Lazy
import           Data.Map (Map)
import qualified Data.Map as Map
import           Data.String (String)

import           Disorder.Jack (Property, Jack)
import           Disorder.Jack (quickCheckAll, gamble, tripping, listOf)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Header


prop_roundtrip_schema :: Property
prop_roundtrip_schema =
  gamble (mapOf jAttributeName jSchema) $
    tripping (Build.toLazyByteString . bHeader) (runGetEither getHeader)

runGetEither :: Get a -> Lazy.ByteString -> Either (Lazy.ByteString, ByteOffset, String) a
runGetEither g =
  let
    third (_, _, x) = x
  in
    second third . Get.runGetOrFail g

mapOf :: Ord k => Jack k -> Jack v -> Jack (Map k v)
mapOf k v =
  Map.fromList <$> listOf ((,) <$> k <*> v)

return []
tests :: IO Bool
tests =
  $quickCheckAll
