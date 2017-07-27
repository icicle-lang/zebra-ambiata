{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Binary.Striped where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble, listOfN)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Viking.ByteStream as ByteStream

import           Zebra.Serial.Binary.Striped
import qualified Zebra.Table.Striped as Striped


data BinaryError =
    BinaryEncode !BinaryStripedEncodeError
  | BinaryDecode !BinaryStripedDecodeError
    deriving (Eq, Show)

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
        (first BinaryEncode . withList (ByteStream.toChunks . encodeStriped))
        (first BinaryDecode . withList (decodeStriped . ByteStream.fromChunks))
        (fmap takeStriped logical)

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
