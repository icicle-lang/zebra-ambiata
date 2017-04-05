{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Json.Schema where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Json.Schema


prop_roundtrip_schema :: Property
prop_roundtrip_schema =
  gamble jTableSchema $
    tripping (encodeSchema SchemaV0) (decodeSchema SchemaV0)

return []
tests :: IO Bool
tests =
  $quickCheckAll
