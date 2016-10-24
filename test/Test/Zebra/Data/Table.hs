{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Table where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping, arbitrary)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Data.Table


prop_roundtrip_splitAt :: Property
prop_roundtrip_splitAt =
  gamble arbitrary $ \ix ->
  gamble jTable $
    tripping (splitAtTable ix) (uncurry appendTables)

return []
tests :: IO Bool
tests =
  $quickCheckAll
