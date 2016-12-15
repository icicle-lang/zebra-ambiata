{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Array where

import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           Disorder.Core.Run
import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, listOf, arbitrary, sizedBounded)

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

prop_roundtrip_ints :: Property
prop_roundtrip_ints =
  gamble (Storable.fromList <$> listOf sizedBounded) $ \xs ->
    trippingSerial bIntArray (getIntArray $ Storable.length xs) xs

prop_roundtrip_zigzag :: Property
prop_roundtrip_zigzag =
  gamble sizedBounded $ \x ->
    x == unZigZag64 (zigZag64 x)

prop_mid64 :: Property
prop_mid64 =
  gamble sizedBounded $ \x ->
  gamble sizedBounded $ \y ->
    fromIntegral (mid64 x y) == midBig (fromIntegral x) (fromIntegral y)

midBig :: Integer -> Integer -> Integer
midBig x y =
  x + (y - x) `div` 2

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunMore
