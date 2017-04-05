{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Factset.Block where

import qualified Data.List as List
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Vector as Boxed

import           Disorder.Core.Run (ExpectedTestSpeed(..), disorderCheckEnvAll)
import           Disorder.Jack (Property, counterexample)
import           Disorder.Jack ((===), gamble, listOf)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           Zebra.Factset.Block
import           Zebra.Factset.Data
import           Zebra.Factset.Table
import qualified Zebra.Table.Striped as Striped


prop_roundtrip_facts :: Property
prop_roundtrip_facts =
  gamble jColumnSchema $ \schema ->
  gamble (listOf $ jFact schema (AttributeId 0)) $ \facts ->
    let
      schemas =
        Boxed.singleton schema

      input =
        Boxed.fromList facts
    in
      trippingBoth (blockOfFacts schemas) factsOfBlock input

prop_roundtrip_tables :: Property
prop_roundtrip_tables =
  gamble jBlock $ \block ->
  let
    names =
      fmap (AttributeName . Text.pack . ("attribute_" <>) . show)
        [0..Boxed.length (blockTables block) - 1]
  in
    trippingBoth (tableOfBlock $ Boxed.fromList names) blockOfTable block

prop_roundtrip_attribute_schemas :: Property
prop_roundtrip_attribute_schemas =
  gamble (listOf jColumnSchema) $ \attrs0 ->
  let
    mkAttr (ix :: Int) attr0 =
      (AttributeName . Text.pack $ "attribute_" <> show ix, attr0)

    attrs =
      Map.fromList $
      List.zipWith mkAttr [0..] attrs0
  in
    trippingBoth (pure . tableSchemaOfAttributes) attributesOfTableSchema attrs

prop_logical_from_block :: Property
prop_logical_from_block =
  gamble jBlock $ \block ->
  let
    names =
      fmap (AttributeName . Text.pack . ("attribute_" <>) . show)
        [0..Boxed.length (blockTables block) - 1]
  in
    either (flip counterexample False) id $ do
      striped0 <- first ppShow $ tableOfBlock (Boxed.fromList names) block

      logical <- first (\x -> ppShow striped0 <> "\n" <> ppShow x) $
        Striped.toLogical striped0

      striped <- first ppShow $ Striped.fromLogical (Striped.schema striped0) logical

      pure . counterexample (ppShow logical) $
        striped0
        ===
        striped

return []
tests :: IO Bool
tests =
  $disorderCheckEnvAll TestRunMore
