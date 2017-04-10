{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Binary.Striped where

import           Disorder.Jack (Property)
import           Disorder.Jack (quickCheckAll, gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Zebra.Serial.Binary.Striped
import qualified Zebra.Table.Striped as Striped


prop_roundtrip_table :: Property
prop_roundtrip_table =
  gamble jBinaryVersion $ \version ->
  gamble (jStriped 1) $ \table ->
    trippingSerialE (bTable version) (getTable version 1 $ Striped.schema table) table

prop_roundtrip_column :: Property
prop_roundtrip_column =
  gamble jBinaryVersion $ \version ->
  gamble (jStripedColumn 1) $ \column ->
    trippingSerialE (bColumn version) (getColumn version 1 $ Striped.schemaColumn column) column

return []
tests :: IO Bool
tests =
  $quickCheckAll
