{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Table.Schema where

import           Disorder.Core.Run (ExpectedTestSpeed(..), disorderCheckEnvAll)
import           Disorder.Jack (Property, (===), gamble, shuffle)

import           P

import           System.IO (IO)

import           Test.QuickCheck (cover)
import           Test.Zebra.Jack

import qualified Zebra.Table.Schema as Schema


prop_union_associative :: Property
prop_union_associative =
  gamble jTableSchema $ \table0 ->
  gamble (jExpandedTableSchema table0) $ \table1 ->
  gamble (jContractedTableSchema table0) $ \table2 ->
  gamble (shuffle [table0, table1, table2]) $ \[x, y, z] ->
    let
      x_yz =
        first (const ())
          (Schema.union x =<< Schema.union y z)

      xy_z =
        first (const ())
          (Schema.union x y >>= \xy -> Schema.union xy z)

      compatible =
        isRight x_yz && isRight xy_z
    in
      -- Check that >90% of test cases are unions on compatible schemas
      cover compatible 90 "compatible schemas" $
        x_yz === xy_z

return []
tests :: IO Bool
tests =
  $disorderCheckEnvAll TestRunMore
