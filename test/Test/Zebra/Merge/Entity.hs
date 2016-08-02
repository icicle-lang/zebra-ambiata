{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
module Test.Zebra.Merge.Entity where

import           Disorder.Jack
import           Disorder.Core.Run

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Data
import           Zebra.Data.Encoding
import           Zebra.Merge.Entity

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Stream as Stream
import qualified Data.List as List
import qualified Data.Map as Map
import qualified Prelude as Savage
import           Text.Show.Pretty (ppShow)

fakeBlockId :: BlockDataId
fakeBlockId = BlockDataId 0

ppCounter :: (Show a, Testable p) => Savage.String -> a -> p -> Property
ppCounter heading thing prop
 = counterexample ("=== " <> heading <> " ===")
 $ counterexample (ppShow thing) prop


jEncodings :: Jack [Encoding]
jEncodings = listOfN 0 5 jEncoding

jFactsFor :: [Encoding] -> Jack [Fact]
jFactsFor encs = sized $ \size -> do
  let encs' = List.zip encs (fmap AttributeId [0..])
  let maxFacts = size `div` length encs
  List.sort . List.concat <$> mapM (\(enc,aid) -> listOfN 0 maxFacts $ jFact enc aid) encs'

blockOfFacts' :: [Encoding] -> [Fact] -> Block
blockOfFacts' encs facts =
  case blockOfFacts (Boxed.fromList encs) (Boxed.fromList facts) of
   Left e -> Savage.error
              ("jBlockFromFacts: invariant failed\n"
              <> "\tgenerated facts cannot be converted to block\n"
              <> "\t" <> show e)
   Right b -> b

jBlockValid :: Jack Block
jBlockValid = do
  encs  <- jEncodings
  blockOfFacts' encs <$> jFactsFor encs


zprop_entitiesOfBlock_entities :: Property
zprop_entitiesOfBlock_entities =
  gamble jBlock $ \block ->
    fmap evEntity (entitiesOfBlock fakeBlockId block) === blockEntities block

zprop_entitiesOfBlock_indices :: Property
zprop_entitiesOfBlock_indices =
  gamble jBlockValid $ \block ->
    catIndices (entitiesOfBlock fakeBlockId block) === takeIndices block
 where
  catIndices evs
   = Boxed.map fst
   $ Boxed.concatMap Boxed.convert
   $ Boxed.concatMap evIndices evs

  takeIndices block
   = Boxed.convert
   $ blockIndices block

zprop_entitiesOfBlock_records_1_entity :: Property
zprop_entitiesOfBlock_records_1_entity =
  gamble jEncodings $ \encs ->
  gamble (jFactsFor encs) $ \facts ->
  gamble jEntityHashId $ \(ehash,eid) ->
  let fixFact f = f { factEntityHash = ehash, factEntityId = eid }
      facts'    = List.sort $ fmap fixFact facts
      block     = blockOfFacts' encs facts'
      es        = entitiesOfBlock fakeBlockId block
  in  ppCounter "Block" block
    $ ppCounter "Entities" es
    ( length facts > 0
    ==> Boxed.concatMap id (getFakeRecordValues es) === blockRecords block )

getFakeRecordValues :: Boxed.Vector EntityValues -> Boxed.Vector (Boxed.Vector Record)
getFakeRecordValues = fmap (fmap (Map.! fakeBlockId) . evRecords)

zprop_mergeEntityRecords_1_block :: Property
zprop_mergeEntityRecords_1_block =
  gamble jBlockValid $ \block ->
  let es = entitiesOfBlock fakeBlockId block
      recs_l = mapM mergeEntityRecords es

      recs_r = getFakeRecordValues es
  in  ppCounter "Entities" es (recs_l === Right recs_r)


prop_mergeEntityRecords_2_blocks :: Property
prop_mergeEntityRecords_2_blocks =
  gamble jEncodings $ \encs ->
  gamble (jFactsFor encs) $ \f1 ->
  gamble (jFactsFor encs) $ \f2 ->
  let b1 = blockOfFacts' encs f1
      b2 = blockOfFacts' encs f2
      bMerge = blockOfFacts' encs $ List.sort (f1 <> f2)

      entsOf bid bk = Stream.streamOfVector $ entitiesOfBlock (BlockDataId bid) bk
      es = Stream.vectorOfStream $ mergeEntityValues (entsOf 1 b1) (entsOf 2 b2)
  in  ppCounter "Block 1" b1
    $ ppCounter "Block 2" b2
    $ ppCounter "Block of append" bMerge
    $ ppCounter "Merged" es
    ( fmap extractEntityValues es === fmap extractEntityValues (entitiesOfBlock fakeBlockId bMerge) )

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunMore

