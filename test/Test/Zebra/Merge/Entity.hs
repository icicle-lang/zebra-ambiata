{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Test.Zebra.Merge.Entity where

import qualified Data.List as List
import qualified Data.Map as Map

import           Disorder.Jack
import           Disorder.Core.Run

import           P

import qualified Prelude as Savage

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Data.Block
import           Zebra.Data.Fact
import           Zebra.Merge.Base
import           Zebra.Merge.Entity
import           Zebra.Schema (ColumnSchema)
import           Zebra.Table (Table(..))


fakeBlockId :: BlockDataId
fakeBlockId = BlockDataId 0

entityValuesOfBlock' :: BlockDataId -> Block -> Boxed.Vector EntityValues
entityValuesOfBlock' blockId block = Stream.vectorOfStream $ entityValuesOfBlock blockId block

ppCounter :: (Show a, Testable p) => Savage.String -> a -> p -> Property
ppCounter heading thing prop
 = counterexample ("=== " <> heading <> " ===")
 $ counterexample (ppShow thing) prop


jColumnSchemas :: Jack [ColumnSchema]
jColumnSchemas = listOfN 0 5 jColumnSchema

blockOfFacts' :: [ColumnSchema] -> [Fact] -> Block
blockOfFacts' schemas facts =
  case blockOfFacts (Boxed.fromList schemas) (Boxed.fromList facts) of
   Left e -> Savage.error
              ("jBlockFromFacts: invariant failed\n"
              <> "\tgenerated facts cannot be converted to block\n"
              <> "\t" <> show e)
   Right b -> b

prop_entitiesOfBlock_entities :: Property
prop_entitiesOfBlock_entities =
  gamble jYoloBlock $ \block ->
    fmap evEntity (entityValuesOfBlock' fakeBlockId block) === blockEntities block

prop_entitiesOfBlock_indices :: Property
prop_entitiesOfBlock_indices =
  gamble jBlock $ \block ->
    catIndices (entityValuesOfBlock' fakeBlockId block) === takeIndices block
 where
  catIndices evs
   = Boxed.map fst
   $ Boxed.concatMap Boxed.convert
   $ Boxed.concatMap evIndices evs

  takeIndices block
   = Boxed.convert
   $ blockIndices block

prop_entitiesOfBlock_tables_1_entity :: Property
prop_entitiesOfBlock_tables_1_entity =
  gamble jColumnSchemas $ \schemas ->
  gamble (jFacts schemas) $ \facts ->
  gamble jEntityHashId $ \(ehash,eid) ->
  let fixFact f = f { factEntityHash = ehash, factEntityId = eid }
      facts'    = List.sort $ fmap fixFact facts
      block     = blockOfFacts' schemas facts'
      es        = entityValuesOfBlock' fakeBlockId block
  in  ppCounter "Block" block
    $ ppCounter "Entities" es
    ( length facts > 0
    ==> Boxed.concatMap id (getFakeTableValues es) === blockTables block )

getFakeTableValues :: Boxed.Vector EntityValues -> Boxed.Vector (Boxed.Vector Table)
getFakeTableValues = fmap (fmap (Map.! fakeBlockId) . evTables)

prop_mergeEntityTables_1_block :: Property
prop_mergeEntityTables_1_block =
  gamble jBlock $ \block ->
  let es = entityValuesOfBlock' fakeBlockId block
      recs_l = mapM mergeEntityTables es

      recs_r = getFakeTableValues es
  in  ppCounter "Entities" es (recs_l === Right recs_r)


prop_treeFold_sum :: Property
prop_treeFold_sum =
  gamble arbitrary $ \(bs :: [Int]) ->
  List.sum bs === treeFold (+) 0 id (Boxed.fromList bs)

prop_treeFold_with_map :: Property
prop_treeFold_with_map =
  gamble arbitrary $ \(bs :: [Int]) ->
  List.sum (fmap (+1) bs) === treeFold (+) 0 (+1) (Boxed.fromList bs)


return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal
