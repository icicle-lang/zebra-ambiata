{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Table where

import qualified Data.Vector as Boxed

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack ((===), gamble, tripping, arbitrary, listOf1, counterexample)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import qualified Zebra.Table as Table


prop_roundtrip_values :: Property
prop_roundtrip_values =
  gamble jSchema $ \schema ->
  gamble (fmap (Boxed.fromList . toList) . listOf1 $ jValue schema) $ \values0 ->
  either (flip counterexample False) id $ do
    table <- first ppShow $ Table.concat =<< traverse (Table.fromRow schema) values0
    values <- first ppShow $ Table.rows schema table
    pure $
      values0
      ===
      values

prop_roundtrip_splitAt :: Property
prop_roundtrip_splitAt =
  gamble arbitrary $ \ix ->
  gamble jAnyTable $
    tripping (Table.splitAt ix) (uncurry Table.append)

return []
tests :: IO Bool
tests =
  $quickCheckAll
