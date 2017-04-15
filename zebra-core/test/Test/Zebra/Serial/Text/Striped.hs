{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Text.Striped where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble, listOfN)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Zebra.ByteStream as ByteStream
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
        (first TextEncode . encodeStripedBlock)
        (first TextDecode . decodeStripedBlock schema)
        striped

prop_roundtrip_file :: Property
prop_roundtrip_file =
  gamble jTableSchema $ \schema ->
  gamble (listOfN 1 10 $ jSizedLogical1 schema) $ \logical ->
    let
      takeStriped x =
        let
          Right striped =
            Striped.fromLogical schema x
        in
          striped
    in
      trippingBoth
        (first TextEncode . withList (ByteStream.toChunks . encodeStriped))
        (first TextDecode . withList (decodeStriped schema . ByteStream.fromChunks))
        (fmap takeStriped logical)

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
