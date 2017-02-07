{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Encoding where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Data.Encoding


prop_roundtrip_encoding :: Property
prop_roundtrip_encoding =
  gamble jEncoding $
    tripping renderEncoding parseEncoding

prop_roundtrip_column_encoding :: Property
prop_roundtrip_column_encoding =
  gamble jColumnEncoding $
    tripping renderColumnEncoding parseColumnEncoding

return []
tests :: IO Bool
tests =
  $quickCheckAll
