{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Record.Mutable where

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


prop_default_record_vs_mrecord :: Property
prop_default_record_vs_mrecord =
  gamble jEncoding $ \encoding ->
    recordOfMaybeValue encoding Nothing' ===
    Right (fromMaybeValue encoding [Nothing'])

prop_roundtrip_value_mrecord :: Property
prop_roundtrip_value_mrecord =
  gamble jEncoding $ \encoding ->
  gamble (listOf $ jValue encoding) $
    tripping (fromMaybeValue encoding . fmap Just') (toValue encoding)

prop_roundtrip_record_mrecord :: Property
prop_roundtrip_record_mrecord =
  gamble jRecord $ \record -> runST $ do
    mrecord <- thawRecord record
    record' <- unsafeFreezeRecord mrecord
    return (record === record')

prop_appendRecord_commutes :: Property
prop_appendRecord_commutes =
  gamble jEncoding $ \encoding ->
  gamble (listOf $ jMaybe' $ jValue encoding) $ \values1 ->
  gamble (listOf $ jMaybe' $ jValue encoding) $ \values2 -> runST $ do
    let rec1 = fromMaybeValue encoding values1
    let rec2 = fromMaybeValue encoding values2
    let rec3 = fromMaybeValue encoding (values1 <> values2)
    mrecord <- thawRecord rec1
    Right () <- runEitherT $ appendRecord mrecord rec2
    rec3' <- unsafeFreezeRecord mrecord
    return (rec3 === rec3')



fromMaybeValue :: Encoding -> [Maybe' Value] -> Record
fromMaybeValue encoding mvalues =
  runST $ do
    record <- newMRecord encoding
    result <- runEitherT $
      traverse_ (insertMaybeValue encoding record) mvalues
    case result of
      Left err ->
        Savage.error $ show err
      Right () ->
        unsafeFreezeRecord record

toValue :: Encoding -> Record -> Either ValueError [Value]
toValue encoding record =
  fmap Boxed.toList $
  valuesOfRecord encoding record

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal
