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
  gamble jEncoding $ \encoding ->
    tableOfMaybeValue encoding Nothing' ===
    Right (fromMaybeValue encoding [Nothing'])

prop_roundtrip_value_mtable :: Property
prop_roundtrip_value_mtable =
  gamble jEncoding $ \encoding ->
  gamble (listOf $ jValue encoding) $
    tripping (fromMaybeValue encoding . fmap Just') (toValue encoding)

prop_roundtrip_table_mtable :: Property
prop_roundtrip_table_mtable =
  gamble jTable $ \table -> runST $ do
    mtable <- thawTable table
    table' <- unsafeFreezeTable mtable
    return (table === table')

prop_appendTable_commutes :: Property
prop_appendTable_commutes =
  gamble jEncoding $ \encoding ->
  gamble (listOf $ jMaybe' $ jValue encoding) $ \values1 ->
  gamble (listOf $ jMaybe' $ jValue encoding) $ \values2 -> runST $ do
    let rec1 = fromMaybeValue encoding values1
    let rec2 = fromMaybeValue encoding values2
    let rec3 = fromMaybeValue encoding (values1 <> values2)
    mtable <- thawTable rec1
    Right () <- runEitherT $ appendTable mtable rec2
    rec3' <- unsafeFreezeTable mtable
    return (rec3 === rec3')



fromMaybeValue :: Encoding -> [Maybe' Value] -> Table
fromMaybeValue encoding mvalues =
  runST $ do
    table <- newMTable encoding
    result <- runEitherT $
      traverse_ (insertMaybeValue encoding table) mvalues
    case result of
      Left err ->
        Savage.error $ show err
      Right () ->
        unsafeFreezeTable table

toValue :: Encoding -> Table -> Either ValueError [Value]
toValue encoding table =
  fmap Boxed.toList $
  valuesOfTable encoding table

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal
