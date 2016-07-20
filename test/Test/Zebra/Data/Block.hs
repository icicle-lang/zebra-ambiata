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
import           Zebra.Data.Fact
import           Zebra.Data.Record


prop_roundtrip_facts :: Property
prop_roundtrip_facts =
  gamble jEncoding $ \encoding ->
  gamble (listOf $ jFact encoding (AttributeId 0)) $ \facts ->
    let
      encodings =
        Boxed.singleton encoding

      input =
        Boxed.fromList facts
    in
      trippingBoth
        (first RecordError . blockOfFacts encodings)
        (first FactError . factsOfBlock encodings)
        input

data SomeError =
    RecordError !RecordError
  | FactError !FactError
    deriving (Eq, Show)

trippingBoth :: (Monad m, Show (m a), Eq (m a)) => (a -> m b) -> (b -> m a) -> a -> Property
trippingBoth to from x =
  let
    roundtrip =
      from =<< to x

    original =
      pure x
  in
    counterexample "" .
    counterexample "Roundtrip failed." .
    counterexample "" .
    counterexample "=== Original ===" .
    counterexample (ppShow original) .
    counterexample "" .
    counterexample "=== Roundtrip ===" .
    counterexample (ppShow roundtrip) $
      property (roundtrip == original)

return []
tests :: IO Bool
tests =
  $quickCheckAll
