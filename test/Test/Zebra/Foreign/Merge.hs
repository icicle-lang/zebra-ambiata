{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Merge where

import qualified Anemone.Foreign.Mempool as Mempool
import           Anemone.Foreign.Segv (withSegv)

import           Control.Monad.Catch (bracket)

import qualified Data.List as List
import qualified Data.Vector as Boxed

import           Disorder.Core.IO (testIO)
import           Disorder.Core.Run (disorderCheckEnvAll, ExpectedTestSpeed(..))
import           Disorder.Corpus (southpark)
import           Disorder.Jack (Property, counterexample)
import           Disorder.Jack (gamble, (===))
import           Disorder.Jack (Jack, sized)
import           Disorder.Jack (listOf, listOfN, maybeOf, choose, elements, scale)

import           P

import           System.IO (IO)

import           Text.Show.Pretty (ppShow)

import           Test.Zebra.Jack

import           X.Control.Monad.Trans.Either (runEitherT, firstEitherT)

import           Zebra.Foreign.Merge
import           Zebra.Foreign.Entity

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Data.Entity
import           Zebra.Data.Fact
import           Zebra.Schema

import qualified Zebra.Merge.Puller.List as TestMerge


--
-- Generators
--

jSchemas :: Jack [Schema]
jSchemas = listOfN 0 5 jSchema

jSmallTime :: Jack Time
jSmallTime = Time <$> choose (0, 100)

jSmallFactsetId :: Jack FactsetId
jSmallFactsetId = FactsetId <$> choose (0, 100)

-- | Fact for a single entity
jFactForEntity :: (EntityHash, EntityId) -> Schema -> AttributeId -> Jack Fact
jFactForEntity eid schema aid =
  uncurry Fact
    <$> pure eid
    <*> pure aid
    <*> jSmallTime
    <*> jSmallFactsetId
    <*> (strictMaybe <$> maybeOf (jValue schema))

-- | Generate a bunch of facts that all have the same entity
-- Used for testing merging the same entity
jFactsFor :: (EntityHash, EntityId) -> [Schema] -> Jack [Fact]
jFactsFor eid schemas = sized $ \size -> do
  let schemas' = List.zip schemas (fmap AttributeId [0..])
  let maxFacts = size `div` length schemas
  sortFacts . List.concat <$> mapM (\(enc,aid) -> listOfN 0 maxFacts $ jFactForEntity eid enc aid) schemas'


jSmallEntity :: Jack (EntityHash, EntityId)
jSmallEntity =
  let
    hash eid =
      EntityHash $ unEntityHash (hashEntityId eid) `mod` 10
  in
    (\eid -> (hash eid, eid)) <$> (EntityId <$> elements southpark)

jSmallFact :: Schema -> AttributeId -> Jack Fact
jSmallFact schema aid =
  uncurry Fact
    <$> jSmallEntity
    <*> pure aid
    <*> jSmallTime
    <*> jSmallFactsetId
    <*> (strictMaybe <$> maybeOf (jValue schema))


jSmallFacts :: [Schema] -> Jack [Fact]
jSmallFacts schemas =
  fmap (sortFacts . List.concat) .
  scale (`div` max 1 (length schemas)) $
  zipWithM (\e a -> listOf $ jSmallFact e a) schemas (fmap AttributeId [0..])

-- Some smallish number of megabytes to garbage collect by
jGarbageCollectEvery :: Jack Int64
jGarbageCollectEvery = choose (0, 10 * 1024 * 1024)




-- | Split a list of facts so it can be treated as two blocks.
-- This has to split on an entity boundary.
-- If the two blocks contained the same entity, it would be bad.
bisectBlockFacts :: Int -> [Fact] -> ([Fact],[Fact])
bisectBlockFacts split fs =
  let heads = List.take split fs
      tails = List.drop split fs
  in case tails of
      [] -> (heads, tails)
      (f:_) ->
        let (heads', tails') = dropLikeEntity f tails
        in (heads <> heads', tails')
 where
  factEntity = (,) <$> factEntityHash <*> factEntityId
  dropLikeEntity f
   = List.span (\f' -> factEntity f == factEntity f')

-- | Generate a pair of facts that can be used as blocks in a single file
jBlockPair :: (FactsetId -> FactsetId) -> [Schema] -> Jack ([Fact],[Fact])
jBlockPair factsetId_mode schemas = do
  fs <- jSmallFacts schemas
  let fs' = fmap (\f -> f { factFactsetId = factsetId_mode $ factFactsetId f }) fs
  split <- choose (0, length fs')
  return $ bisectBlockFacts split fs'

-- | Construct a C Block from a bunch of facts
testForeignOfFacts :: Mempool.Mempool -> [Schema] -> [Fact] -> IO (Block Schema, Boxed.Vector CEntity)
testForeignOfFacts pool schemas facts = do
  let Right block    = blockOfFacts (Boxed.fromList schemas) (Boxed.fromList facts)
  let Right entities = entitiesOfBlock block
  es' <- mapM (foreignOfEntity pool) entities
  return (block, es')

-- | This is the slow obvious implementation to check against.
-- It sorts all the facts by entity and turns them into entities.
-- This must be a stable sort.
testMergeFacts :: [Schema] -> [Fact] -> Boxed.Vector (Entity Schema)
testMergeFacts schemas facts =
  let Right block    = blockOfFacts (Boxed.fromList schemas) (Boxed.fromList $ sortFacts facts)
      Right entities = entitiesOfBlock block
  in  entities

sortFacts :: [Fact] -> [Fact]
sortFacts = List.sortBy cmp
 where
  cmp a b
   = sortbits a `compare` sortbits b
  -- We are basically sorting by everything except the value
  sortbits f
   = (factEntityHash f, factEntityId f, factAttributeId f, factTime f, negate $ factFactsetId f)

-- | Merge facts for same entity. We should not segfault
prop_merge_1_entity_no_segfault :: Property
prop_merge_1_entity_no_segfault =
  gamble jSmallEntity $ \eid ->
  gamble jSchemas $ \schemas ->
  gamble (jFactsFor eid schemas) $ \facts1 ->
  gamble (jFactsFor eid schemas) $ \facts2 ->
  testIO . bracket Mempool.create Mempool.free $ \pool -> withSegv (ppShow (eid, schemas, facts1, facts2)) $ do
    (b1,cs1) <- testForeignOfFacts pool schemas facts1
    (b2,cs2) <- testForeignOfFacts pool schemas facts2
    let cs' = Boxed.zip cs1 cs2
    merged <- runEitherT $ mapM (uncurry $ mergeEntityPair pool) cs'
    -- Only checking segfault for now
    return $ counterexample (ppShow (b1,b2))
        $ case merged of
           Right _ -> True
           Left _ -> False

-- | Merge facts for same entity. We should get the right result
-- This is stable - if two facts have the same entity, time and factsetId, favour the first
prop_merge_1_entity_check_result :: Property
prop_merge_1_entity_check_result =
  gamble jSmallEntity $ \eid ->
  gamble jSchemas $ \schemas ->
  gamble (jFactsFor eid schemas) $ \facts1 ->
  gamble (jFactsFor eid schemas) $ \facts2 ->
  testIO . bracket Mempool.create Mempool.free $ \pool -> withSegv (ppShow (eid, schemas, facts1, facts2)) $ do
    let expect = testMergeFacts schemas (facts1 <> facts2)
    (b1,cs1) <- testForeignOfFacts pool schemas facts1
    (b2,cs2) <- testForeignOfFacts pool schemas facts2
    let cs' = Boxed.zip cs1 cs2
    let err i = firstEitherT ppShow i
    merged <- runEitherT $ do
      ms <- err $ mapM (uncurry $ mergeEntityPair pool) cs'
      err $ mapM entityOfForeign ms

    return $ counterexample (ppShow (b1,b2))
           $ case merged of
              Right m'
                | Boxed.length cs1 == 1 && Boxed.length cs2 == 1
                -> fmap (() <$) expect === m'
                | otherwise
                -> Boxed.empty === m'
              Left  e' -> counterexample e' False


-- | Merge two blocks from two different files (one block each)
-- This is stable since we know upfront that all blocks will have their entities added
-- in a particular order.
prop_merge_1_block_2_files :: Property
prop_merge_1_block_2_files =
  gamble jSchemas $ \schemas ->
  gamble (jSmallFacts schemas) $ \facts1 ->
  gamble (jSmallFacts schemas) $ \facts2 ->
  gamble jGarbageCollectEvery $ \gcEvery ->
  testIO . withSegv (ppShow (schemas, facts1, facts2)) $ do
    let expect = fmap (() <$) $ testMergeFacts schemas (facts1 <> facts2)
    let Right b1 = blockOfFacts (Boxed.fromList schemas) (Boxed.fromList facts1)
    let Right b2 = blockOfFacts (Boxed.fromList schemas) (Boxed.fromList facts2)
    let err i = firstEitherT ppShow i
    merged <- runEitherT $ err $ TestMerge.mergeLists gcEvery [[b1], [b2]]

    return $ counterexample (ppShow (b1,b2))
           $ case merged of
              Right m'
                -> Boxed.toList expect === m'
              Left  e' -> counterexample e' False

-- | Merge four blocks from two different files.
-- This should be sufficient to show it works for many files and many blocks,
-- since this tests the whole replenishment/refill loop anyway.
--
-- This is NOT stable because the block boundaries can be anywhere.
-- That means we can have the values from entity Z from file 2 block 1, then later
-- decide to read file 1 block 2, which also has values for entity Z.
--
-- Because this is not stable, we make sure there are no facts with the same key.
-- We do this by just generating even factsetIds for the left and odd factsetIds for the right.
-- This will still give us interesting sorting cases, but ensure no duplicates.
prop_merge_2_block_2_files :: Property
prop_merge_2_block_2_files =
  gamble jSchemas $ \schemas ->
  gamble (jBlockPair evenFactsetId schemas) $ \(facts11, facts12) ->
  gamble (jBlockPair oddFactsetId  schemas) $ \(facts21, facts22) ->
  gamble jGarbageCollectEvery $ \gcEvery ->
  testIO . withSegv (ppShow (schemas, (facts11,facts12), (facts21, facts22))) $ do
    let mkBlock = blockOfFacts (Boxed.fromList schemas) . Boxed.fromList
    let expect = fmap (() <$) $ testMergeFacts schemas (facts11 <> facts12 <> facts21 <> facts22)

    let Right b11 = mkBlock facts11
    let Right b12 = mkBlock facts12
    let Right b21 = mkBlock facts21
    let Right b22 = mkBlock facts22
    let allBlocks = [ [b11, b12], [b21, b22] ]

    let err i = firstEitherT ppShow i
    merged <- runEitherT $ err $ TestMerge.mergeLists gcEvery $ allBlocks

    return $ counterexample (ppShow allBlocks)
           $ case merged of
              Right m'
                -> Boxed.toList expect === m'
              Left  e' -> counterexample e' False
 where
  evenFactsetId (FactsetId i) = FactsetId (i * 2)
  oddFactsetId (FactsetId i) = FactsetId (i * 2 + 1)


return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal

