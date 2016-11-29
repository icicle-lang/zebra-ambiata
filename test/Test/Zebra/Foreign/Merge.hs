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
import           Disorder.Jack (gamble, (===))
import           Disorder.Jack (Jack, sized)
import           Disorder.Jack (listOfN, maybeOf)

import           P

import qualified Data.List as List
import qualified Data.Vector as Boxed
import           System.IO (IO)
import           X.Control.Monad.Trans.Either (runEitherT, firstEitherT)
import           Text.Show.Pretty (ppShow)

import           Test.Zebra.Jack
import qualified Test.Zebra.Merge.BlockC as TestMerge

import           Zebra.Foreign.Merge
import           Zebra.Foreign.Entity

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Data.Encoding
import           Zebra.Data.Entity
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


testForeignOfFacts :: Mempool.Mempool -> [Encoding] -> [Fact] -> IO (Block, Boxed.Vector CEntity)
testForeignOfFacts pool encs facts = do
  let Right block    = blockOfFacts (Boxed.fromList encs) (Boxed.fromList facts)
  let Right entities = entitiesOfBlock block
  es' <- mapM (foreignOfEntity pool) entities
  return (block, es')

testMergeFacts :: [Encoding] -> [Fact] -> Boxed.Vector Entity
testMergeFacts encs facts =
  let Right block    = blockOfFacts (Boxed.fromList encs) (Boxed.fromList $ List.sort facts)
      Right entities = entitiesOfBlock block
  in  entities


zprop_merge_1_entity_no_segfault :: Property
zprop_merge_1_entity_no_segfault =
  gamble jEntityHashId $ \eid ->
  gamble jEncodings $ \encs ->
  gamble (jFactsFor eid encs) $ \facts1 ->
  gamble (jFactsFor eid encs) $ \facts2 ->
  testIO . bracket Mempool.create Mempool.free $ \pool -> withSegv (ppShow (eid, encs, facts1, facts2)) $ do
    (b1,cs1) <- testForeignOfFacts pool encs facts1
    (b2,cs2) <- testForeignOfFacts pool encs facts2
    let cs' = Boxed.zip cs1 cs2
    merged <- runEitherT $ mapM (uncurry $ mergeEntity pool) cs'
    -- Only checking segfault for now
    return $ counterexample (ppShow (b1,b2))
        $ case merged of
           Right _ -> True
           Left _ -> False

zprop_merge_1_entity_check_result :: Property
zprop_merge_1_entity_check_result =
  gamble jEntityHashId $ \eid ->
  gamble jEncodings $ \encs ->
  gamble (jFactsFor eid encs) $ \facts1 ->
  gamble (jFactsFor eid encs) $ \facts2 ->
  testIO . bracket Mempool.create Mempool.free $ \pool -> withSegv (ppShow (eid, encs, facts1, facts2)) $ do
    let expect = testMergeFacts encs (facts1 <> facts2)
    (b1,cs1) <- testForeignOfFacts pool encs facts1
    (b2,cs2) <- testForeignOfFacts pool encs facts2
    let cs' = Boxed.zip cs1 cs2
    let err i = firstEitherT ppShow i
    merged <- runEitherT $ do
      ms <- err $ mapM (uncurry $ mergeEntity pool) cs'
      err $ mapM entityOfForeign ms

    return $ counterexample (ppShow (b1,b2))
           $ case merged of
              Right m'
                | Boxed.length cs1 == 1 && Boxed.length cs2 == 1
                -> expect === m'
                | otherwise
                -> Boxed.empty === m'
              Left  e' -> counterexample e' False

prop_merge_1_block_2_files :: Property
prop_merge_1_block_2_files =
  gamble jEncodings $ \encs ->
  gamble (jFacts encs) $ \facts1 ->
  gamble (jFacts encs) $ \facts2 ->
  testIO . withSegv (ppShow (encs, facts1, facts2)) $ do
    let expect = testMergeFacts encs (facts1 <> facts2)
    let Right b1 = blockOfFacts (Boxed.fromList encs) (Boxed.fromList facts1)
    let Right b2 = blockOfFacts (Boxed.fromList encs) (Boxed.fromList facts2)
    let err i = firstEitherT ppShow i
    merged <- runEitherT $ err $ TestMerge.mergeLists [[b1], [b2]]

    return $ counterexample (ppShow (b1,b2))
           $ case merged of
              Right m'
                -> Boxed.toList expect === m'
              Left  e' -> counterexample e' False


return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal

