{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Value where

import           Disorder.Jack (Property)
import           Disorder.Jack ((===), (==>), quickCheckAll, gamble)

import qualified Data.Vector as Boxed

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Value


prop_reversed :: Property
prop_reversed =
  gamble jColumnSchema $ \schema ->
  gamble (jValue schema) $ \x ->
  gamble (jValue schema) $ \y ->
    compare x y
    ===
    compare (Reversed y) (Reversed x)

prop_nested :: Property
prop_nested =
  gamble jColumnSchema $ \schema ->
  gamble (jValue schema) $ \x ->
  gamble (jValue schema) $ \y ->
    compare x y
    ===
    compare (Nested (Array (Boxed.singleton x))) (Nested (Array (Boxed.singleton y)))

prop_ord_0 :: Property
prop_ord_0 =
  gamble jColumnSchema $ \schema ->
  gamble (jValue schema) $ \x ->
  gamble (jValue schema) $ \y ->
  gamble (jValue schema) $ \z ->
    x > y && y > z
    ==>
    x > z

prop_ord_1 :: Property
prop_ord_1 =
  gamble jColumnSchema $ \schema ->
  gamble (jValue schema) $ \x ->
  gamble (jValue schema) $ \y ->
  gamble (jValue schema) $ \z ->
    x < y && y < z
    ==>
    x < z

return []
tests :: IO Bool
tests =
  $quickCheckAll
