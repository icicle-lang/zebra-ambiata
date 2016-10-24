{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Block where

import qualified Anemone.Foreign.Mempool as Mempool

import           Control.Monad.Catch (bracket)

import           Disorder.Core.IO (testIO)
import           Disorder.Jack (Property)
import           Disorder.Jack (quickCheckAll, gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Zebra.Foreign.Block


prop_roundtrip_blocks :: Property
prop_roundtrip_blocks =
  gamble jBlock $ \block ->
  testIO . bracket Mempool.create Mempool.free $ \pool ->
    trippingIO (foreignOfBlock pool) blockOfForeign block

return []
tests :: IO Bool
tests =
  $quickCheckAll
