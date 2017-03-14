{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Block where

import qualified Anemone.Foreign.Mempool as Mempool
import           Anemone.Foreign.Segv (withSegv)

import           Control.Monad.Catch (bracket)

import           Data.String (String)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed

import           Disorder.Core.IO (testIO)
import           Disorder.Core.Run (disorderCheckEnvAll, ExpectedTestSpeed(..))
import           Disorder.Jack (Property)
import           Disorder.Jack ((===), gamble, counterexample, conjoin)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Text.Show.Pretty (ppShow)

import           X.Control.Monad.Trans.Either (EitherT, pattern EitherT, runEitherT, hoistEither)

import           Zebra.Data.Block
import           Zebra.Data.Entity
import qualified Zebra.Data.Entity as Entity
import           Zebra.Foreign.Block
import           Zebra.Foreign.Entity
import           Zebra.Foreign.Util
import qualified Zebra.Table as Table


data CommonError =
    -- Invalid inputs can be wrong in multiple ways at the same, different
    -- backends find different things first, this is ok.
    ExpectedError
  | UnexpectedError String
    deriving (Eq, Ord, Show)

fromEntityError :: EntityError -> CommonError
fromEntityError = \case
  EntityAttributeNotFound _ ->
    ExpectedError
  EntityNotEnoughRows ->
    ExpectedError

fromForeignError :: ForeignError -> CommonError
fromForeignError = \case
  ForeignAttributeNotFound ->
    ExpectedError
  ForeignNotEnoughRows ->
    ExpectedError
  err ->
    UnexpectedError $ show err

foreignEntitiesOfBlock' :: Block -> EitherT ForeignError IO (Boxed.Vector Entity)
foreignEntitiesOfBlock' block =
  EitherT .
  bracket Mempool.create Mempool.free $ \pool ->
  runEitherT $ do
    fblock <- foreignOfBlock pool block
    fentities <- foreignEntitiesOfBlock pool fblock
    traverse entityOfForeign fentities

foreignBlockOfEntities' :: Boxed.Vector Entity -> EitherT ForeignError IO (Maybe Block)
foreignBlockOfEntities' entities =
  EitherT .
  bracket Mempool.create Mempool.free $ \pool ->
  runEitherT $ do
    fentities <- traverse (foreignOfEntity pool) entities
    fblock <- foldM (\fb fe -> Just <$> appendEntityToBlock pool fe fb) Nothing fentities
    traverse blockOfForeign fblock

check :: (Eq a, Eq x, Show a, Show x) => String -> a -> Either x a -> Property
check header v0 ev1 =
  counterexample "" .
  counterexample ("=== " <> header <> " ===") .
  counterexample "- Original" .
  counterexample "+ After Conversion" .
  counterexample "" $
  case ev1 of
    Left _ ->
      Right v0 === ev1
    Right v1 ->
      v0 === v1

check_entities_of_block :: (Block -> EitherT CommonError IO (Boxed.Vector Entity)) -> Block -> Property
check_entities_of_block convert block =
  testIO . withSegv (ppShow block) $ do
    entities <- runEitherT $ convert block

    let
      entityCount :: Int
      entityCount =
        Boxed.length $ blockEntities block

      recordCount :: Int
      recordCount =
        Unboxed.length $ blockIndices block

      sumAttribute :: (Attribute -> Int) -> Boxed.Vector Entity -> Int
      sumAttribute f es =
        Boxed.sum $
        Boxed.map (Boxed.sum . Boxed.map f . Entity.entityAttributes) es

    pure $ conjoin [
        check "# of entities" entityCount $
          fmap Boxed.length entities

      , check "# of time rows" recordCount $
          fmap (sumAttribute $ Storable.length . attributeTime) entities

      , check "# of factsetId rows" recordCount $
          fmap (sumAttribute $ Storable.length . attributeFactsetId) entities

      , check "# of tombstone rows" recordCount $
          fmap (sumAttribute $ Storable.length . attributeTombstone) entities

      , check "# of table rows" recordCount $
          fmap (sumAttribute $ Table.length . attributeTable) entities
      ]

fromEither :: Show x => EitherT x IO Property -> IO Property
fromEither =
  fmap (either (\x -> counterexample (ppShow x) False) id) . runEitherT

check_block_of_entities :: Block -> Property
check_block_of_entities block =
  testIO . withSegv (ppShow block) . fromEither $ do
    entities <- firstT (\x -> (Boxed.empty, x)) $ foreignEntitiesOfBlock' block
    block' <- firstT (\x -> (entities, x)) $ foreignBlockOfEntities' entities

    let expect
          | Boxed.null (blockEntities block)
          = Nothing
          | otherwise
          = Just block

    return .
      counterexample "=== Entities ===" .
      counterexample (ppShow entities) $
      expect === block'


check_c_vs_haskell :: Block -> Property
check_c_vs_haskell block =
  testIO . withSegv (ppShow block) $ do
    entities1 <-
      runEitherT .
      firstT fromForeignError $
      foreignEntitiesOfBlock' block

    let
      entities2 =
        first fromEntityError $
        entitiesOfBlock block

    pure $
      counterexample "" .
      counterexample "- C" .
      counterexample "+ Haskell" .
      counterexample "" $
      entities1 === entities2

prop_compare_entities_of_block_valid :: Property
prop_compare_entities_of_block_valid =
  gamble jBlock check_c_vs_haskell

prop_compare_entities_of_block_yolo :: Property
prop_compare_entities_of_block_yolo =
  gamble jYoloBlock check_c_vs_haskell

prop_haskell_entities_of_block :: Property
prop_haskell_entities_of_block =
  gamble jBlock . check_entities_of_block $
    firstT fromEntityError . hoistEither . entitiesOfBlock

prop_c_entities_of_block :: Property
prop_c_entities_of_block =
  gamble jBlock . check_entities_of_block $
    firstT fromForeignError . foreignEntitiesOfBlock'

prop_c_block_of_entities :: Property
prop_c_block_of_entities =
  gamble jBlock $ check_block_of_entities

prop_roundtrip_blocks :: Property
prop_roundtrip_blocks =
  gamble jYoloBlock $ \block ->
  testIO . bracket Mempool.create Mempool.free $ \pool ->
    trippingIO (liftE . foreignOfBlock pool) blockOfForeign block

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunMore
