{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Table.Mutable where

import           Control.Monad.ST (runST)

import           Disorder.Core.Run (disorderCheckEnvAll, ExpectedTestSpeed(..))
import           Disorder.Jack (Property)
import           Disorder.Jack ((===), gamble, tripping, listOf)

import qualified Data.Vector as Boxed

import           P

import qualified Prelude as Savage

import           System.IO (IO)

import           Test.Zebra.Jack

import           X.Control.Monad.Trans.Either (runEitherT)

import           Zebra.Data


prop_default_table_vs_mtable :: Property
prop_default_table_vs_mtable =
  gamble jSchema $ \schema ->
    tableOfMaybeValue schema Nothing' ===
    Right (fromMaybeValue schema [Nothing'])

prop_roundtrip_value_mtable :: Property
prop_roundtrip_value_mtable =
  gamble jSchema $ \schema ->
  gamble (listOf $ jValue schema) $
    tripping (fromMaybeValue schema . fmap Just') (toValue schema)

prop_roundtrip_table_mtable :: Property
prop_roundtrip_table_mtable =
  gamble jTable $ \table -> runST $ do
    mtable <- thawTable table
    table' <- unsafeFreezeTable mtable
    return (table === table')

prop_appendTable_commutes :: Property
prop_appendTable_commutes =
  gamble jSchema $ \schema ->
  gamble (listOf $ jMaybe' $ jValue schema) $ \values1 ->
  gamble (listOf $ jMaybe' $ jValue schema) $ \values2 -> runST $ do
    let rec1 = fromMaybeValue schema values1
    let rec2 = fromMaybeValue schema values2
    let rec3 = fromMaybeValue schema (values1 <> values2)
    mtable <- thawTable rec1
    Right () <- runEitherT $ appendTable mtable rec2
    rec3' <- unsafeFreezeTable mtable
    return (rec3 === rec3')



fromMaybeValue :: Schema -> [Maybe' Value] -> Table
fromMaybeValue schema mvalues =
  runST $ do
    table <- newMTable schema
    result <- runEitherT $
      traverse_ (insertMaybeValue schema table) mvalues
    case result of
      Left err ->
        Savage.error $ show err
      Right () ->
        unsafeFreezeTable table

toValue :: Schema -> Table -> Either ValueError [Value]
toValue schema table =
  fmap Boxed.toList $
  valuesOfTable schema table

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal
