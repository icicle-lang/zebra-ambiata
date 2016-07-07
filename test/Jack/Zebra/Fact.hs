{-# LANGUAGE NoImplicitPrelude #-}
module Jack.Zebra.Fact (
    jAttributeName
  ) where

import           Disorder.Jack (Jack, elements, oneOf, arbitrary)
import           Disorder.Corpus (muppets)

import           P

import           Test.QuickCheck.Instances ()

import           Zebra.Fact


jAttributeName :: Jack AttributeName
jAttributeName =
  AttributeName <$> oneOf [elements muppets, arbitrary]
