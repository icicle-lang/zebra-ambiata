{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Array where

import           Data.Binary.Get (Get, ByteOffset)
import qualified Data.Binary.Get as Get
import qualified Data.ByteString.Builder as Build
import qualified Data.ByteString.Lazy as Lazy
import           Data.String (String)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping, listOf, arbitrary, sizedBounded)

import           P

import           System.IO (IO)

import           Test.QuickCheck.Instances ()

import           Zebra.Array


prop_roundtrip_strings :: Property
prop_roundtrip_strings =
  gamble (Boxed.fromList <$> listOf arbitrary) $ \xs ->
    tripping
      (Build.toLazyByteString . bStrings)
      (runGetEither . getStrings $ Boxed.length xs)
      xs

prop_roundtrip_bytes :: Property
prop_roundtrip_bytes =
  gamble arbitrary $
    tripping
      (Build.toLazyByteString . bByteArray)
      (runGetEither $ getByteArray)

prop_roundtrip_words :: Property
prop_roundtrip_words =
  gamble (Storable.fromList <$> listOf sizedBounded) $ \xs ->
    tripping
      (Build.toLazyByteString . bWordArray)
      (runGetEither . getWordArray $ Storable.length xs)
      xs

runGetEither :: Get a -> Lazy.ByteString -> Either (Lazy.ByteString, ByteOffset, String) a
runGetEither g =
  let
    third (_, _, x) = x
  in
    second third . Get.runGetOrFail g

return []
tests :: IO Bool
tests =
  $quickCheckAll
