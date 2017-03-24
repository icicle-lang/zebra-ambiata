{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Binary.Header where

import           Data.Map (Map)
import qualified Data.Map as Map

import           Disorder.Jack (Property, Jack)
import           Disorder.Jack (quickCheckAll, gamble, listOf, oneOf)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Zebra.Binary.Header


prop_roundtrip_header_v2 :: Property
prop_roundtrip_header_v2 =
  gamble (mapOf jAttributeName jColumnSchema) $
    trippingSerial bHeaderV2 getHeaderV2

prop_roundtrip_header_v3 :: Property
prop_roundtrip_header_v3 =
  gamble jTableSchema $
    trippingSerial bHeaderV3 getHeaderV3

prop_roundtrip_header :: Property
prop_roundtrip_header =
  gamble jHeader $
    trippingSerial bHeader getHeader

jHeader :: Jack Header
jHeader =
  oneOf [
      HeaderV3 <$> jTableSchema
    , HeaderV2 <$> mapOf jAttributeName jColumnSchema
    ]

mapOf :: Ord k => Jack k -> Jack v -> Jack (Map k v)
mapOf k v =
  Map.fromList <$> listOf ((,) <$> k <*> v)

return []
tests :: IO Bool
tests =
  $quickCheckAll
