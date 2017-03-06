{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Schema where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Zebra.Schema as Schema


prop_roundtrip_schema :: Property
prop_roundtrip_schema =
  gamble jSchema $
    tripping Schema.encode Schema.decode

return []
tests :: IO Bool
tests =
  $quickCheckAll
