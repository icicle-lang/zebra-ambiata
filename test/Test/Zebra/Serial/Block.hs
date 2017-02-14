{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Block where

import qualified Data.Vector as Boxed
import qualified Data.Vector.Unboxed as Unboxed

import           Disorder.Jack (Property)
import           Disorder.Jack (quickCheckAll, gamble, listOf, counterexample)

import           P

import qualified Prelude as Savage

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Text.Show.Pretty (ppShow)

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Data.Encoding
import           Zebra.Data.Table
import           Zebra.Serial.Block


prop_roundtrip_from_facts :: Property
prop_roundtrip_from_facts =
  gamble jSchema $ \schema ->
  gamble (listOf $ jFact schema (AttributeId 0)) $ \facts ->
    let
      schemas =
        Boxed.singleton schema

      encoding =
        fmap encodingOfSchema schemas

      block =
        either (Savage.error . show) id .
        blockOfFacts schemas $
        Boxed.fromList facts
    in
      counterexample (ppShow schema) $
      trippingSerial bBlock (getBlock encoding) block

prop_roundtrip_block :: Property
prop_roundtrip_block =
  gamble jYoloBlock $ \block ->
    trippingSerial bBlock (getBlock . fmap encodingOfTable $ blockTables block) block

prop_roundtrip_entities :: Property
prop_roundtrip_entities =
  gamble (Boxed.fromList <$> listOf jBlockEntity) $
    trippingSerial bEntities getEntities

prop_roundtrip_attributes :: Property
prop_roundtrip_attributes =
  gamble (Unboxed.fromList <$> listOf jBlockAttribute) $
    trippingSerial bAttributes getAttributes

prop_roundtrip_indices :: Property
prop_roundtrip_indices =
  gamble (Unboxed.fromList <$> listOf jBlockIndex) $
    trippingSerial bIndices getIndices

prop_roundtrip_tables :: Property
prop_roundtrip_tables =
  gamble (Boxed.fromList <$> listOf jTable) $ \xs ->
    trippingSerial bTables (getTables $ fmap encodingOfTable xs) xs

prop_roundtrip_table :: Property
prop_roundtrip_table =
  gamble (jTable' 1) $ \table ->
    trippingSerial bTable (getTable 1 $ encodingOfTable table) table

prop_roundtrip_column :: Property
prop_roundtrip_column =
  gamble (jColumn 1) $ \column ->
    trippingSerial bColumn (getColumn 1 $ encodingOfColumn column) column

return []
tests :: IO Bool
tests =
  $quickCheckAll
