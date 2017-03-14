{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Entity where

import qualified Anemone.Foreign.Mempool as Mempool

import           Control.Monad.Catch (bracket)

import           Disorder.Core.IO (testIO)
import           Disorder.Jack (Property)
import           Disorder.Jack (quickCheckAll, gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Zebra.Foreign.Entity


prop_roundtrip_entity :: Property
prop_roundtrip_entity =
  gamble jEntity $ \entity ->
  testIO . bracket Mempool.create Mempool.free $ \pool ->
    trippingIO (liftE . foreignOfEntity pool) entityOfForeign entity

return []
tests :: IO Bool
tests =
  $quickCheckAll
