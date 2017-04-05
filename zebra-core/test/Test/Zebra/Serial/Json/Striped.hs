{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Json.Striped where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Json.Striped
import qualified Zebra.Table.Striped as Striped


data JsonError =
    JsonEncode !JsonStripedEncodeError
  | JsonDecode !JsonStripedDecodeError
    deriving (Eq, Show)

prop_roundtrip_table :: Property
prop_roundtrip_table =
  gamble jTableSchema $ \schema ->
  gamble (jSizedLogical schema) $ \logical ->
    let
      Right striped =
        Striped.fromLogical schema logical
    in
      trippingBoth
        (first JsonEncode . encodeStriped)
        (first JsonDecode . decodeStriped schema)
        striped

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
