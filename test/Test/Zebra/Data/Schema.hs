{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Schema where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Data.Schema


prop_roundtrip_schema :: Property
prop_roundtrip_schema =
  gamble jSchema $
    tripping renderSchema parseSchema

return []
tests :: IO Bool
tests =
  $quickCheckAll
