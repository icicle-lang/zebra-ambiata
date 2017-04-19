{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Binary.Logical where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble, listOfN)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Zebra.ByteStream as ByteStream
import           Zebra.Serial.Binary.Logical
import qualified Zebra.Stream as Stream


data BinaryError =
    BinaryEncode !BinaryLogicalEncodeError
  | BinaryDecode !BinaryLogicalDecodeError
    deriving (Eq, Show)

prop_roundtrip_file :: Property
prop_roundtrip_file =
  gamble jTableSchema $ \schema ->
  gamble (listOfN 1 10 $ jSizedLogical1 schema) $ \logical ->
    trippingBoth
      (first BinaryEncode . withList (ByteStream.toChunks . encodeLogical schema))
      (first BinaryDecode . withList (Stream.effect . fmap snd . decodeLogical . ByteStream.fromChunks))
      logical

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
