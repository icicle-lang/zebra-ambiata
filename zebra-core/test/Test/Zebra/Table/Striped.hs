{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Table.Striped where

import           Disorder.Jack (Property)
import           Disorder.Jack ((===), gamble, tripping, arbitrary, counterexample, discard)
import           Disorder.Jack (listOf1, choose, conjoin)
import           Disorder.Core (ExpectedTestSpeed(..), disorderCheckEnvAll)

import           Data.Functor.Identity (runIdentity)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (runEitherT)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Cons as Cons

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

prop_rechunk :: Property
prop_rechunk =
  gamble (choose (1, 100)) $ \n ->
  gamble (Cons.fromNonEmpty <$> listOf1 jSizedStriped) $ \xss ->
    let
      original =
        Striped.unsafeConcat xss

      rechunked =
        bind (Striped.unsafeConcat . Cons.unsafeFromList) $
          runIdentity .
          runEitherT .
          Stream.toList_ .
          Striped.rechunk n $
          Stream.each xss
    in
      original === rechunked

prop_default_table_check :: Property
prop_default_table_check =
  gamble jTableSchema $ \schema ->
    let
      striped =
        Striped.defaultTable schema
      logical =
        Logical.defaultTable schema
    in
      conjoin [
          counterexample "=== Compare Striped ===" $
            Striped.toLogical striped === Right logical
        , counterexample "=== Compare Logical ===" $
            Striped.fromLogical schema logical === Right striped
        ]

prop_default_column_check :: Property
prop_default_column_check =
  gamble (choose (0, 100)) $ \n ->
  gamble jColumnSchema $ \schema ->
    let
      striped =
        Striped.defaultColumn n schema
      logical =
        Boxed.replicate n $ Logical.defaultValue schema
    in
      conjoin [
          counterexample "=== Compare Striped ===" $
            Striped.fromValues schema logical === Right striped
        , counterexample "=== Compare Logical ===" $
            Striped.toValues striped === Right logical
        ]

prop_transmute_identity :: Property
prop_transmute_identity =
  gamble jSizedStriped $ \table ->
    Striped.transmute (Striped.schema table) table === Right table

prop_transmute_expand :: Property
prop_transmute_expand =
  gamble jSizedStriped $ \table0 ->
  let
    schema0 =
      Striped.schema table0
  in
    gamble (jExpandedTableSchema schema0) $ \schema ->
      conjoin [
          counterexample "=== Compare Schema ===" $
            (Striped.schema <$> Striped.transmute schema table0) === Right schema

        , counterexample "=== Roundtrip Table ===" $
            (Striped.transmute schema0 =<< Striped.transmute schema table0) === Right table0
        ]

prop_transmute_merge :: Property
prop_transmute_merge =
  counterexample "=== Schema ===" $
  gamble jTableSchema $ \schema0 ->
  counterexample "=== Expanded Schema ===" $
  gamble (jExpandedTableSchema schema0) $ \schema ->
  counterexample "=== Logical 0 ===" $
  gamble (jSizedLogical schema0) $ \logical0 ->
  counterexample "=== Logical 1 ===" $
  gamble (jSizedLogical schema0) $ \logical1 ->
  let
    Right striped0 =
      Striped.fromLogical schema0 logical0

    Right striped1 =
      Striped.fromLogical schema0 logical1
  in
    counterexample "=== Striped 0 ===" .
    counterexample (ppShow striped0) .
    counterexample "=== Striped 1 ===" .
    counterexample (ppShow striped1) $
    testEither $ do
      tm <-
        Striped.transmute schema . discardLeft $
          Striped.merge striped0 striped1

      mtt <- do
        t0 <- Striped.transmute schema striped0
        t1 <- Striped.transmute schema striped1
        pure . discardLeft $ Striped.merge t0 t1

      pure $
        tm === mtt

return []
tests :: IO Bool
tests =
  $disorderCheckEnvAll TestRunMore
