{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Json.Table where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Zebra.Table as Table
import           Zebra.Json.Table


data JsonError =
    JsonEncode !JsonTableEncodeError
  | JsonDecode !JsonTableDecodeError
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
        (first JsonEncode . encodeTable)
        (first JsonDecode . decodeTable schema)
        table

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
