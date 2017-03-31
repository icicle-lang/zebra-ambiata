{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Json.Codec where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text

import           Disorder.Jack (Property, Jack, forAllProperties, quickCheckWithResult, maxSuccess, stdArgs)
import           Disorder.Jack (gamble, tripping, once, arbitrary, boundedEnum, sizedBounded)
import           Disorder.Jack (listOf, choose, chooseChar, suchThat)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Json.Codec
import           Zebra.Json.Schema


jJsonVersion :: Jack JsonVersion
jJsonVersion =
  boundedEnum

jText :: Jack Text
jText =
  fmap Text.pack . listOf $
    chooseChar (minBound, maxBound)

jBinary :: Jack ByteString
jBinary =
  fmap ByteString.pack . listOf $
    choose (minBound, maxBound)

prop_roundtrip_version :: Property
prop_roundtrip_version =
  gamble jJsonVersion $
    tripping (encodeJson [] . ppVersion) (decodeJson pVersion)

prop_roundtrip_unit :: Property
prop_roundtrip_unit =
  once $
    tripping (\_ -> encodeJson [] ppUnit) (decodeJson pUnit) ()

prop_roundtrip_int :: Property
prop_roundtrip_int =
  gamble sizedBounded $
    tripping (encodeJson [] . ppInt) (decodeJson pInt)

prop_roundtrip_double :: Property
prop_roundtrip_double =
  gamble arbitrary $
    tripping (encodeJson [] . ppDouble) (decodeJson pDouble)

prop_roundtrip_text :: Property
prop_roundtrip_text =
  gamble jText $
    tripping (encodeJson [] . ppText) (decodeJson pText)

prop_roundtrip_binary :: Property
prop_roundtrip_binary =
  gamble jBinary $
    tripping (encodeJson [] . ppBinary) (decodeJson pBinary)

prop_roundtrip_utf8 :: Property
prop_roundtrip_utf8 =
  gamble (suchThat jBinary $ isRight . Text.decodeUtf8') $
    tripping Text.decodeUtf8 (Just . Text.encodeUtf8)

return []
tests :: IO Bool
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 10000})
