{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Text.Value where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Text.Value


data TextError =
    TextEncode !TextValueEncodeError
  | TextDecode !TextValueDecodeError
    deriving (Eq, Show)

prop_roundtrip_collection :: Property
prop_roundtrip_collection =
  gamble jTableSchema $ \schema ->
  gamble (jSizedCollection schema) $
    trippingBoth
      (first TextEncode . encodeCollection schema)
      (first TextDecode . decodeCollection schema)

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
