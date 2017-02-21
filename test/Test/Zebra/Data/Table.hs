{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Table where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping, arbitrary)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Zebra.Data.Table as Table


prop_roundtrip_splitAt :: Property
prop_roundtrip_splitAt =
  gamble arbitrary $ \ix ->
  gamble jAnyTable $
    tripping (Table.splitAt ix) (uncurry Table.append)

return []
tests :: IO Bool
tests =
  $quickCheckAll
