{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Entity where

import qualified Anemone.Foreign.Mempool as Mempool

import           Control.Monad.Catch (bracket)

import           Disorder.Jack (Property)
import           Disorder.Core.IO (testIO)
import           Disorder.Jack (quickCheckAll, gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Zebra.Foreign.Entity


prop_roundtrip_entities :: Property
prop_roundtrip_entities =
  gamble jEntity $ \entity ->
  testIO . bracket Mempool.create Mempool.free $ \pool ->
    trippingIO (foreignOfEntity pool) entityOfForeign entity

return []
tests :: IO Bool
tests =
  $quickCheckAll
