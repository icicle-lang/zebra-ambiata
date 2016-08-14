{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Array where

import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           Disorder.Jack (Property)
import           Disorder.Jack (gamble, listOf, arbitrary, sizedBounded)
import           Disorder.Core.Run

import           P

import           System.IO (IO)

import           Test.QuickCheck.Instances ()
import           Test.Zebra.Util

import           Zebra.Serial.Array


prop_roundtrip_strings :: Property
prop_roundtrip_strings =
  gamble (Boxed.fromList <$> listOf arbitrary) $ \xs ->
    trippingSerial bStrings (getStrings $ Boxed.length xs) xs

prop_roundtrip_bytes :: Property
prop_roundtrip_bytes =
  gamble arbitrary $
    trippingSerial bByteArray getByteArray

prop_roundtrip_words :: Property
prop_roundtrip_words =
  gamble (Storable.fromList <$> listOf sizedBounded) $ \xs ->
    trippingSerial bWordArray (getWordArray $ Storable.length xs) xs

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal
