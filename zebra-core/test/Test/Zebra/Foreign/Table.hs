{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Table where

import           Anemone.Foreign.Mempool (Mempool)
import qualified Anemone.Foreign.Mempool as Mempool

import           Control.Monad.Catch (bracket)

import           Disorder.Core.IO (testIO)
import           Disorder.Core.Run (ExpectedTestSpeed(..), disorderCheckEnvAll)
import           Disorder.Jack (Property)
import           Disorder.Jack (gamble, chooseInt)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           X.Control.Monad.Trans.Either (EitherT)

import           Zebra.Foreign.Table
import           Zebra.Table.Striped (Table)
import qualified Zebra.Table.Striped as Striped

prop_roundtrip_table :: Property
prop_roundtrip_table =
  gamble jSizedStriped $ \table ->
  testIO . bracket Mempool.create Mempool.free $ \pool ->
    trippingIO (liftE . foreignOfTable pool) tableOfForeign table

prop_deep_clone_table :: Property
prop_deep_clone_table =
  testClone id deepCloneTable

prop_neritic_clone_table :: Property
prop_neritic_clone_table =
  testClone id neriticCloneTable

prop_agile_clone_table :: Property
prop_agile_clone_table =
  testClone Striped.schema agileCloneTable

prop_grow_table :: Property
prop_grow_table =
  gamble (chooseInt (0, 100)) $ \n ->
    testClone Striped.schema (\pool table -> growTable pool table n >> pure table)

testClone :: (Show a, Show x, Eq a, Eq x) => (Table -> a) -> (Mempool -> CTable -> EitherT x IO CTable) -> Property
testClone select clone =
  gamble jSizedStriped $ \table ->
  testIO . bracket Mempool.create Mempool.free $ \pool ->
    trippingByIO select (bind (clone pool) . foreignOfTable pool) tableOfForeign table

return []
tests :: IO Bool
tests =
  $disorderCheckEnvAll TestRunMore
