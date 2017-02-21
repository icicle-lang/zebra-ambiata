{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Table.Mutable where

import           Control.Monad.ST (runST)

import           Disorder.Core.Run (disorderCheckEnvAll, ExpectedTestSpeed(..))
import           Disorder.Jack (Property)
import           Disorder.Jack ((===), gamble, tripping, listOf, counterexample)

import qualified Data.Vector as Boxed

import           P

import qualified Prelude as Savage

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           X.Control.Monad.Trans.Either (runEitherT)

import           Zebra.Data.Fact (Value)
import           Zebra.Data.Schema (Schema)
import           Zebra.Data.Table (Table, ValueError)
import qualified Zebra.Data.Table as Table
import qualified Zebra.Data.Table.Mutable as MTable


prop_default_table_vs_mtable :: Property
prop_default_table_vs_mtable =
  gamble jSchema $ \schema ->
  let
    table =
      Table.fromRowOrDefault schema Nothing'
  in
    counterexample (either ppShow ppShow table) $
    table === Right (fromMaybeValue schema [Nothing'])

prop_roundtrip_value_mtable :: Property
prop_roundtrip_value_mtable =
  gamble jSchema $ \schema ->
  gamble (listOf $ jValue schema) $
    tripping (fromMaybeValue schema . fmap Just') (toValue schema)

prop_roundtrip_table_mtable :: Property
prop_roundtrip_table_mtable =
  gamble jAnyTable $ \table -> runST $ do
    mtable <- MTable.thaw table
    table' <- MTable.unsafeFreeze mtable
    return (table === table')

prop_appendTable_commutes :: Property
prop_appendTable_commutes =
  gamble jSchema $ \schema ->
  gamble (listOf $ jMaybe' $ jValue schema) $ \values1 ->
  gamble (listOf $ jMaybe' $ jValue schema) $ \values2 -> runST $ do
    let rec1 = fromMaybeValue schema values1
    let rec2 = fromMaybeValue schema values2
    let rec3 = fromMaybeValue schema (values1 <> values2)
    mtable <- MTable.thaw rec1
    Right () <- runEitherT $ MTable.append mtable rec2
    rec3' <- MTable.unsafeFreeze mtable
    return (rec3 === rec3')

fromMaybeValue :: Schema -> [Maybe' Value] -> Table Schema
fromMaybeValue schema mvalues =
  runST $ do
    table <- MTable.new schema
    result <- runEitherT $
      traverse_ (MTable.insertRowOrDefault table) mvalues
    case result of
      Left err ->
        Savage.error $ show err
      Right () ->
        MTable.unsafeFreeze table

toValue :: Schema -> Table a -> Either (ValueError a) [Value]
toValue schema table =
  fmap Boxed.toList $
    Table.rows schema table

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal
