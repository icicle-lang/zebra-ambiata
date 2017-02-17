{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Block where

import qualified Data.Vector as Boxed

import           Disorder.Jack (Property, quickCheckAll, counterexample)
import           Disorder.Jack (gamble, property, listOf)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Data.Schema (Schema)
import           Zebra.Data.Table.Mutable (MutableError)


prop_roundtrip_facts :: Property
prop_roundtrip_facts =
  gamble jSchema $ \schema ->
  gamble (listOf $ jFact schema (AttributeId 0)) $ \facts ->
    let
      schemas =
        Boxed.singleton schema

      input =
        Boxed.fromList facts
    in
      trippingBoth
        (first TableError . blockOfFacts schemas)
        (first FactError . factsOfBlock schemas)
        input

data SomeError =
    TableError !MutableError
  | FactError !(FactError Schema)
    deriving (Eq, Show)

trippingBoth :: (Monad m, Show (m a), Show (m b), Eq (m a)) => (a -> m b) -> (b -> m a) -> a -> Property
trippingBoth to from x =
  let
    original =
      pure x

    intermediate =
      to x

    roundtrip =
      from =<< intermediate
  in
    counterexample "" .
    counterexample "Roundtrip failed." .
    counterexample "" .
    counterexample "=== Original ===" .
    counterexample (ppShow original) .
    counterexample "" .
    counterexample "=== Intermediate ===" .
    counterexample (ppShow intermediate) .
    counterexample "" .
    counterexample "=== Roundtrip ===" .
    counterexample (ppShow roundtrip) $
      property (roundtrip == original)

return []
tests :: IO Bool
tests =
  $quickCheckAll
