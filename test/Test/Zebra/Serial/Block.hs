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
import           Zebra.Data.Schema
import           Zebra.Data.Table
import           Zebra.Serial.Block


prop_roundtrip_from_facts :: Property
prop_roundtrip_from_facts =
  gamble jEncoding $ \encoding ->
  gamble (listOf $ jFact encoding (AttributeId 0)) $ \facts ->
    let
      encodings =
        Boxed.singleton encoding

      schema =
        fmap schemaOfEncoding encodings

      block =
        either (Savage.error . show) id .
        blockOfFacts encodings $
        Boxed.fromList facts
    in
      counterexample (ppShow schema) $
      trippingSerial bBlock (getBlock schema) block

prop_roundtrip_block :: Property
prop_roundtrip_block =
  gamble jYoloBlock $ \block ->
    trippingSerial bBlock (getBlock . fmap schemaOfTable $ blockTables block) block

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
    trippingSerial bTables (getTables $ fmap schemaOfTable xs) xs

prop_roundtrip_table :: Property
prop_roundtrip_table =
  gamble (jTable' 1) $ \table ->
    trippingSerial bTable (getTable 1 $ schemaOfTable table) table

prop_roundtrip_column :: Property
prop_roundtrip_column =
  gamble (jColumn 1) $ \column ->
    trippingSerial bColumn (getColumn 1 $ schemaOfColumn column) column

return []
tests :: IO Bool
tests =
  $quickCheckAll
