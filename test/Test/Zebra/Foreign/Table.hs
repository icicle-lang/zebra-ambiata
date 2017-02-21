{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Table where

import qualified Anemone.Foreign.Mempool as Mempool

import           Control.Monad.Catch (bracket)

import           Disorder.Core.IO (testIO)
import           Disorder.Jack (Property)
import           Disorder.Jack (quickCheckAll, gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Zebra.Foreign.Table


prop_roundtrip_table :: Property
prop_roundtrip_table =
  gamble jAnyTable $ \table ->
  testIO . bracket Mempool.create Mempool.free $ \pool ->
    trippingIO (liftE . foreignOfTable pool) tableOfForeign (() <$ table)

return []
tests :: IO Bool
tests =
  $quickCheckAll
