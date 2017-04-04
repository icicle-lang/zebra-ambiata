{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Text.Table where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Zebra.Table as Table
import           Zebra.Text.Table


data TextError =
    TextEncode !TextTableEncodeError
  | TextDecode !TextTableDecodeError
    deriving (Eq, Show)

prop_roundtrip_table :: Property
prop_roundtrip_table =
  gamble jTableSchema $ \schema ->
  gamble (jSizedCollection schema) $ \collection ->
    let
      Right table =
        Table.fromCollection schema collection
    in
      trippingBoth
        (first TextEncode . encodeTable)
        (first TextDecode . decodeTable schema)
        table

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
