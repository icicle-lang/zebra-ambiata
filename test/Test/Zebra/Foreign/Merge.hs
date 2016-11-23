{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Merge where

import qualified Anemone.Foreign.Mempool as Mempool
import           Anemone.Foreign.Segv (withSegv)

import           Control.Monad.Catch (bracket)

import           Disorder.Core.IO (testIO)
import           Disorder.Core.Run (disorderCheckEnvAll, ExpectedTestSpeed(..))
import           Disorder.Jack (Property)
import           Disorder.Jack (gamble)
import           Disorder.Jack (Jack, sized)
import           Disorder.Jack (listOfN, maybeOf)

import           P

import qualified Data.List as List
import qualified Data.Vector as Boxed
import           System.IO (IO)
import           X.Control.Monad.Trans.Either (runEitherT)
import           Text.Show.Pretty (ppShow)

import           Test.Zebra.Jack

import           Zebra.Foreign.Merge
import           Zebra.Foreign.Entity

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Data.Encoding
import           Zebra.Data.Fact


jEncodings :: Jack [Encoding]
jEncodings = listOfN 0 5 jEncoding

jFactForEntity :: (EntityHash, EntityId) -> Encoding -> AttributeId -> Jack Fact
jFactForEntity eid encoding aid =
  uncurry Fact
    <$> pure eid
    <*> pure aid
    <*> jTime
    <*> jPriority
    <*> (strictMaybe <$> maybeOf (jValue encoding))

jFactsFor :: (EntityHash, EntityId) -> [Encoding] -> Jack [Fact]
jFactsFor eid encs = sized $ \size -> do
  let encs' = List.zip encs (fmap AttributeId [0..])
  let maxFacts = size `div` length encs
  List.sort . List.concat <$> mapM (\(enc,aid) -> listOfN 0 maxFacts $ jFactForEntity eid enc aid) encs'


foreignOfFacts :: Mempool.Mempool -> [Encoding] -> [Fact] -> IO (Boxed.Vector CEntity)
foreignOfFacts pool encs facts = do
  let Right block    = blockOfFacts (Boxed.fromList encs) (Boxed.fromList facts)
  let Right entities = entitiesOfBlock block
  mapM (foreignOfEntity pool) entities


prop_merge_1_entity_no_segfault :: Property
prop_merge_1_entity_no_segfault =
  gamble jEntityHashId $ \eid ->
  gamble jEncodings $ \encs ->
  gamble (jFactsFor eid encs) $ \facts1 ->
  gamble (jFactsFor eid encs) $ \facts2 ->
  testIO . bracket Mempool.create Mempool.free $ \pool -> withSegv (ppShow (eid, encs, facts1, facts2)) $ do
    cs1 <- foreignOfFacts pool encs facts1
    cs2 <- foreignOfFacts pool encs facts2
    let cs' = Boxed.zip cs1 cs2
    Right _ <- runEitherT $ mapM (uncurry $ mergeEntity pool) cs'
    return True

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal

