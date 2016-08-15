{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Header where

import           Data.Map (Map)
import qualified Data.Map as Map

import           Disorder.Jack (Property, Jack)
import           Disorder.Jack (quickCheckAll, gamble, listOf)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Zebra.Serial.Header


prop_roundtrip_schema :: Property
prop_roundtrip_schema =
  gamble (mapOf jAttributeName jSchema) $
    trippingSerial bHeader getHeader

mapOf :: Ord k => Jack k -> Jack v -> Jack (Map k v)
mapOf k v =
  Map.fromList <$> listOf ((,) <$> k <*> v)

return []
tests :: IO Bool
tests =
  $quickCheckAll
