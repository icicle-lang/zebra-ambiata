{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Merge where

import qualified Anemone.Foreign.Mempool as Mempool
import           Anemone.Foreign.Segv (withSegv)

import           Control.Monad.Catch (bracket)

import           Disorder.Core.IO (testIO)
import           Disorder.Core.Run (disorderCheckEnvAll, ExpectedTestSpeed(..))
import           Disorder.Jack (Property, counterexample)
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
jEncodings = listOfN 2 2 jEncoding

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


foreignOfFacts :: Mempool.Mempool -> [Encoding] -> [Fact] -> IO (Block, Boxed.Vector CEntity)
foreignOfFacts pool encs facts = do
  let Right block    = blockOfFacts (Boxed.fromList encs) (Boxed.fromList facts)
  let Right entities = entitiesOfBlock block
  es' <- mapM (foreignOfEntity pool) entities
  return (block, es')


prop_merge_1_entity_no_segfault :: Property
prop_merge_1_entity_no_segfault =
  gamble jEntityHashId $ \eid ->
  gamble jEncodings $ \encs ->
  gamble (jFactsFor eid encs) $ \facts1 ->
  gamble (jFactsFor eid encs) $ \facts2 ->
  testIO . bracket Mempool.create Mempool.free $ \pool -> withSegv (ppShow (eid, encs, facts1, facts2)) $ do
    (b1,cs1) <- foreignOfFacts pool encs facts1
    (b2,cs2) <- foreignOfFacts pool encs facts2
    let cs' = Boxed.zip cs1 cs2
    merged <- runEitherT $ mapM (uncurry $ mergeEntity pool) cs'
    -- Only checking segfault for now
    return $ counterexample (ppShow (b1,b2))
        $ case merged of
           Right _ -> True
           Left _ -> True

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal

