{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Fact where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Data.Fact


prop_roundtrip_day :: Property
prop_roundtrip_day =
  gamble jDay $
    tripping fromDay (Just . toDay)

return []
tests :: IO Bool
tests =
  $quickCheckAll
