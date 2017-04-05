{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Json.Logical where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Json.Logical


data JsonError =
    JsonEncode !JsonLogicalEncodeError
  | JsonDecode !JsonLogicalDecodeError
    deriving (Eq, Show)

prop_roundtrip_table :: Property
prop_roundtrip_table =
  gamble jTableSchema $ \schema ->
  gamble (jSizedLogical schema) $
    trippingBoth
      (first JsonEncode . encodeLogical schema)
      (first JsonDecode . decodeLogical schema)

prop_roundtrip_value :: Property
prop_roundtrip_value =
  gamble jColumnSchema $ \schema ->
  gamble (jLogicalValue schema) $
    trippingBoth
      (first JsonEncode . encodeLogicalValue schema)
      (first JsonDecode . decodeLogicalValue schema)

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
