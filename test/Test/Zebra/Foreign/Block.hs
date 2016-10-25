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
import           Disorder.Jack (Property)
import           Disorder.Jack ((===), quickCheckAll, gamble, counterexample, conjoin)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Text.Show.Pretty (ppShow)

import           X.Control.Monad.Trans.Either (EitherT, pattern EitherT, runEitherT, hoistEither)

import           Zebra.Data.Block
import           Zebra.Data.Entity
import qualified Zebra.Data.Entity as Entity
import           Zebra.Data.Table
import           Zebra.Foreign.Block
import           Zebra.Foreign.Entity
import           Zebra.Foreign.Util


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

check_conversion :: (Block -> EitherT CommonError IO (Boxed.Vector Entity)) -> Block -> Property
check_conversion convert block =
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

      , check "# of priority rows" recordCount $
          fmap (sumAttribute $ Storable.length . attributePriority) entities

      , check "# of tombstone rows" recordCount $
          fmap (sumAttribute $ Storable.length . attributeTombstone) entities

      , check "# of table rows" recordCount $
          fmap (sumAttribute $ rowsOfTable . attributeTable) entities
      ]

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
  gamble jBlock . check_conversion $
    firstT fromEntityError . hoistEither . entitiesOfBlock

prop_c_entities_of_block :: Property
prop_c_entities_of_block =
  gamble jBlock . check_conversion $
    firstT fromForeignError . foreignEntitiesOfBlock'

prop_roundtrip_blocks :: Property
prop_roundtrip_blocks =
  gamble jYoloBlock $ \block ->
  testIO . bracket Mempool.create Mempool.free $ \pool ->
    trippingIO (liftE . foreignOfBlock pool) blockOfForeign block

return []
tests :: IO Bool
tests =
  $quickCheckAll
