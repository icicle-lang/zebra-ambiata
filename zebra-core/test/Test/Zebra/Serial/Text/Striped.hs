{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Text.Striped where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Text.Striped
import qualified Zebra.Table.Striped as Striped


data TextError =
    TextEncode !TextStripedEncodeError
  | TextDecode !TextStripedDecodeError
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
        (first TextEncode . encodeStriped)
        (first TextDecode . decodeStriped schema)
        striped

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
