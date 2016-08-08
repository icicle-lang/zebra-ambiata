{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Record.Mutable where

import           Control.Monad.ST (runST)

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack ((===), gamble, tripping, listOf)

import qualified Data.Vector as Boxed

import           P

import qualified Prelude as Savage

import           System.IO (IO)

import           Test.Zebra.Jack

import           X.Control.Monad.Trans.Either (runEitherT)

import           Zebra.Data


prop_roundtrip_mrecord :: Property
prop_roundtrip_mrecord =
  gamble jEncoding $ \encoding ->
  gamble (listOf $ jValue encoding) $
    tripping (fromMaybeValue encoding . fmap Just') (toValue encoding)

prop_default_record_vs_mrecord :: Property
prop_default_record_vs_mrecord =
  gamble jEncoding $ \encoding ->
    recordOfMaybeValue encoding Nothing' ===
    Right (fromMaybeValue encoding [Nothing'])

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
tests =
  $quickCheckAll
