{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Schema where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping)

import           Jack.Zebra.Schema

import           P

import           System.IO (IO)

import           Zebra.Schema


prop_roundtrip_schema :: Property
prop_roundtrip_schema =
  gamble jSchema $
    tripping renderSchema parseSchema

return []
tests :: IO Bool
tests =
  $quickCheckAll
