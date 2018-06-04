{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Factset.Data where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping)

import           P

import           System.IO (IO)

import           Test.Zebra.JackFactsets

import           Zebra.Factset.Data


prop_roundtrip_day :: Property
prop_roundtrip_day =
  gamble jFactsetDay $
    tripping fromDay (Just . toDay)

return []
tests :: IO Bool
tests =
  $quickCheckAll
