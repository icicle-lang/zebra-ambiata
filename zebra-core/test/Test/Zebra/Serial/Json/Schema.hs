{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Json.Schema where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Json.Schema


prop_roundtrip_schema_v0 :: Property
prop_roundtrip_schema_v0 =
  gamble (tableSchemaV0 <$> jTableSchema) $
    trippingBoth (pure . encodeSchema SchemaV0) (decodeSchema SchemaV0)

prop_roundtrip_schema_v1 :: Property
prop_roundtrip_schema_v1 =
  gamble jTableSchema $
    trippingBoth (pure . encodeSchema SchemaV1) (decodeSchema SchemaV1)

return []
tests :: IO Bool
tests =
  $quickCheckAll
