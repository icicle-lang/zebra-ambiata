{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Text.Logical where

import           Disorder.Jack (Property, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble, listOfN)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Text.Logical
import qualified Zebra.X.ByteStream as ByteStream


data TextError =
    TextEncode !TextLogicalEncodeError
  | TextDecode !TextLogicalDecodeError
    deriving (Eq, Show)

prop_roundtrip_table :: Property
prop_roundtrip_table =
  gamble jTableSchema $ \schema ->
  gamble (jSizedLogical schema) $
    trippingBoth
      (first TextEncode . encodeLogicalBlock schema)
      (first TextDecode . decodeLogicalBlock schema)

prop_roundtrip_file :: Property
prop_roundtrip_file =
  gamble jTableSchema $ \schema ->
  gamble (listOfN 1 10 $ jSizedLogical1 schema) $ \logical ->
    trippingBoth
      (first TextEncode . withList (ByteStream.toChunks . encodeLogical schema))
      (first TextDecode . withList (decodeLogical schema . ByteStream.fromChunks))
      logical

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
