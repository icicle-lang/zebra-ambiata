{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Table where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack ((===), gamble, tripping, arbitrary, counterexample, discard)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import qualified Zebra.Schema as Schema
import qualified Zebra.Table as Table
import qualified Zebra.Value as Value


prop_roundtrip_values :: Property
prop_roundtrip_values =
  gamble jTableSchema $ \schema ->
  gamble (jSizedCollection schema) $ \collection0 ->
  either (flip counterexample False) id $ do
    table <- first ppShow $ Table.fromCollection schema collection0
    collection <- first ppShow $ Table.toCollection table
    pure . counterexample (ppShow table) $
      collection0
      ===
      collection

prop_append_array_table :: Property
prop_append_array_table =
  gamble (Schema.Array <$> jColumnSchema) $ \schema ->
  gamble (jSizedCollection schema) $ \collection0 ->
  gamble (jSizedCollection schema) $ \collection1 ->
  either (flip counterexample False) id $ do
    table0 <- first ppShow $ Table.fromCollection schema collection0
    table1 <- first ppShow $ Table.fromCollection schema collection1
    table <- first ppShow $ Table.unsafeAppend table0 table1

    let
      viaCollection =
        either (const discard) id $
          Value.union collection0 collection1

    viaTable <- first ppShow $ Table.toCollection table

    pure .
      counterexample "== Table 1 ==" .
      counterexample (ppShow table0) .
      counterexample "== Table 2 ==" .
      counterexample (ppShow table1) .
      counterexample "== Table Union ==" .
      counterexample (ppShow table) $
      counterexample "== Table Union / Collection ==" .
      counterexample (ppShow viaTable) $
      counterexample "== Collection Union ==" .
      counterexample (ppShow viaCollection) $
        viaCollection
        ===
        viaTable

prop_roundtrip_splitAt :: Property
prop_roundtrip_splitAt =
  gamble arbitrary $ \ix ->
  gamble jSizedTable $
    tripping (Table.splitAt ix) (uncurry Table.unsafeAppend)

return []
tests :: IO Bool
tests =
  $quickCheckAll
