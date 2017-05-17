{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Table.Striped where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack ((===), gamble, tripping, arbitrary, counterexample, discard)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           Zebra.Table.Data
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped


prop_roundtrip_values :: Property
prop_roundtrip_values =
  gamble jTableSchema $ \schema ->
  gamble (jSizedLogical schema) $ \logical0 ->
  either (flip counterexample False) id $ do
    striped <- first ppShow $ Striped.fromLogical schema logical0
    collection <- first ppShow $ Striped.toLogical striped
    pure . counterexample (ppShow striped) $
      logical0
      ===
      collection

prop_append_array_table :: Property
prop_append_array_table =
  gamble (Schema.Array DenyDefault <$> jColumnSchema) $ \schema ->
  gamble (jSizedLogical schema) $ \logical0 ->
  gamble (jSizedLogical schema) $ \logical1 ->
  either (flip counterexample False) id $ do
    striped0 <- first ppShow $ Striped.fromLogical schema logical0
    striped1 <- first ppShow $ Striped.fromLogical schema logical1
    striped <- first ppShow $ Striped.unsafeAppend striped0 striped1

    let
      viaLogical =
        either (const discard) id $
          Logical.merge logical0 logical1

    viaStriped <- first ppShow $ Striped.toLogical striped

    pure .
      counterexample "== Striped 1 ==" .
      counterexample (ppShow striped0) .
      counterexample "== Striped 2 ==" .
      counterexample (ppShow striped1) .
      counterexample "== Striped Append ==" .
      counterexample (ppShow striped) $
      counterexample "== Striped Append --> Logical ==" .
      counterexample (ppShow viaStriped) $
      counterexample "== Logical Merge ==" .
      counterexample (ppShow viaLogical) $
        viaLogical
        ===
        viaStriped

prop_roundtrip_splitAt :: Property
prop_roundtrip_splitAt =
  gamble arbitrary $ \ix ->
  gamble jSizedStriped $
    tripping (Striped.splitAt ix) (uncurry Striped.unsafeAppend)

return []
tests :: IO Bool
tests =
  $quickCheckAll
