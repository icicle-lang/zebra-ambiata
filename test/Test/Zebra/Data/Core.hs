{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Core where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack ((===), gamble, tripping, once)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Data.Core
import           Zebra.Foreign.Bindings (pattern C'ZEBRA_HASH_SEED)


prop_roundtrip_day :: Property
prop_roundtrip_day =
  gamble jDay $
    tripping fromDay (Just . toDay)

prop_hash_seed :: Property
prop_hash_seed =
  once $ hashSeed === C'ZEBRA_HASH_SEED

return []
tests :: IO Bool
tests =
  $quickCheckAll
