{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Block where

import qualified Data.Vector as Boxed
import qualified Data.Vector.Unboxed as Unboxed

import           Disorder.Jack (Property)
import           Disorder.Jack (gamble, listOf, counterexample)
import           Disorder.Core.Run

import           P

import qualified Prelude as Savage

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Text.Show.Pretty (ppShow)

import           Zebra.Data.Block
import           Zebra.Data.Fact
import           Zebra.Data.Record
import           Zebra.Data.Schema
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
  gamble jBlock $ \block ->
    trippingSerial bBlock (getBlock . fmap schemaOfRecord $ blockRecords block) block

prop_roundtrip_entities :: Property
prop_roundtrip_entities =
  gamble (Boxed.fromList <$> listOf jEntity) $
    trippingSerial bEntities getEntities

prop_roundtrip_attributes :: Property
prop_roundtrip_attributes =
  gamble (Unboxed.fromList <$> listOf jAttribute) $
    trippingSerial bAttributes getAttributes

prop_roundtrip_indices :: Property
prop_roundtrip_indices =
  gamble (Unboxed.fromList <$> listOf jIndex) $
    trippingSerial bIndices getIndices

prop_roundtrip_records :: Property
prop_roundtrip_records =
  gamble (Boxed.fromList <$> listOf jRecord) $ \xs ->
    trippingSerial bRecords (getRecords $ fmap schemaOfRecord xs) xs

prop_roundtrip_record :: Property
prop_roundtrip_record =
  gamble (jRecord' 1) $ \record ->
    trippingSerial bRecord (getRecord 1 $ schemaOfRecord record) record

prop_roundtrip_field :: Property
prop_roundtrip_field =
  gamble (jField 1) $ \field ->
    trippingSerial bField (getField 1 $ schemaOfField field) field

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal
