{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Block where

import qualified Data.List as List
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Vector as Boxed

import           Disorder.Core.Run (ExpectedTestSpeed(..), disorderCheckEnvAll)
import           Disorder.Jack (Property, counterexample)
import           Disorder.Jack ((===), gamble, property, listOf)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           Zebra.Data.Block
import           Zebra.Data.Core
import qualified Zebra.Table as Table


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

prop_collection_from_block :: Property
prop_collection_from_block =
  gamble jBlock $ \block ->
  let
    names =
      fmap (AttributeName . Text.pack . ("attribute_" <>) . show)
        [0..Boxed.length (blockTables block) - 1]
  in
    either (flip counterexample False) id $ do
      table0 <- first ppShow $ tableOfBlock (Boxed.fromList names) block

      collection <- first (\x -> ppShow table0 <> "\n" <> ppShow x) $
        Table.toCollection table0

      table <- first ppShow $ Table.fromCollection (Table.schema table0) collection

      pure . counterexample (ppShow collection) $
        table0
        ===
        table

trippingBoth :: (Monad m, Show (m a), Show (m b), Eq (m a)) => (a -> m b) -> (b -> m a) -> a -> Property
trippingBoth to from x =
  let
    original =
      pure x

    intermediate =
      to x

    roundtrip =
      from =<< intermediate
  in
    counterexample "" .
    counterexample "Roundtrip failed." .
    counterexample "" .
    counterexample "=== Intermediate ===" .
    counterexample (ppShow intermediate) .
    counterexample "" $
      property (original === roundtrip)

return []
tests :: IO Bool
tests =
  $disorderCheckEnvAll TestRunMore
