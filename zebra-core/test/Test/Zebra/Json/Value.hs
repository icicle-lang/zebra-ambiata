{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Json.Value where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Json.Value


data JsonError =
    JsonEncode !JsonValueEncodeError
  | JsonDecode !JsonValueDecodeError
    deriving (Eq, Show)

prop_roundtrip_collection :: Property
prop_roundtrip_collection =
  gamble jTableSchema $ \schema ->
  gamble (jSizedCollection schema) $
    trippingBoth
      (first JsonEncode . encodeCollection schema)
      (first JsonDecode . decodeCollection schema)

prop_roundtrip_value :: Property
prop_roundtrip_value =
  gamble jColumnSchema $ \schema ->
  gamble (jValue schema) $
    trippingBoth
      (first JsonEncode . encodeValue schema)
      (first JsonDecode . decodeValue schema)

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
