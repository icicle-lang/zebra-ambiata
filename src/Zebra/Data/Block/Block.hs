{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Block.Block (
    Block(..)
  , FactError(..)

  , blockOfFacts
  , factsOfBlock
  ) where

import           Control.Monad.State.Strict (MonadState(..))
import           Control.Monad.Trans.State.Strict (State, runState)

import           Data.Typeable (Typeable)

import           GHC.Generics (Generic)

import           P

import           X.Control.Monad.Trans.Either (EitherT, runEitherT, left)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed

import           Zebra.Data.Block.Entity
import           Zebra.Data.Block.Index
import           Zebra.Data.Core
import           Zebra.Data.Encoding
import           Zebra.Data.Fact
import           Zebra.Data.Table
import           Zebra.Data.Table.Mutable


data Block =
  Block {
      blockEntities :: !(Boxed.Vector BlockEntity)
    , blockIndices :: !(Unboxed.Vector BlockIndex)
    , blockTables :: !(Boxed.Vector Table)
    } deriving (Eq, Ord, Show, Generic, Typeable)

data FactError =
    FactValueError !ValueError
  | FactIndicesExhausted
  | FactValuesExhausted !AttributeId
  | FactNoValues !AttributeId
  | FactLeftoverIndices !(Unboxed.Vector BlockIndex)
  | FactLeftoverValues !(Boxed.Vector (Boxed.Vector Value))
    deriving (Eq, Ord, Show, Generic, Typeable)

blockOfFacts :: Boxed.Vector Encoding -> Boxed.Vector Fact -> Either MutableError Block
blockOfFacts encodings facts =
  Block (entitiesOfFacts facts) (indicesOfFacts facts) <$> tablesOfFacts encodings facts

factsOfBlock :: Boxed.Vector Encoding -> Block -> Either FactError (Boxed.Vector Fact)
factsOfBlock encodings block = do
  let
    entities =
      blockEntities block
    indices =
      blockIndices block
    tables =
      blockTables block

  values <- first FactValueError $ Boxed.zipWithM valuesOfTable encodings tables

  let
    (result, ValueState indices' values') =
      runState (runEitherT $ takeEntityFacts entities) (ValueState indices values)

  if not $ Unboxed.null indices' then
    Left $ FactLeftoverIndices indices'
  else if all (not . Boxed.null) values' then
    Left $ FactLeftoverValues values'
  else
    result

data ValueState =
  ValueState {
      _stateIndices :: Unboxed.Vector BlockIndex
    , _stateValues :: Boxed.Vector (Boxed.Vector Value)
    }

takeEntityFacts :: Boxed.Vector BlockEntity -> EitherT FactError (State ValueState) (Boxed.Vector Fact)
takeEntityFacts entities =
  concatFor entities $ \(BlockEntity ehash eid attrs) ->
    -- The conversion from unboxed to boxed is not ideal here, but this
    -- function is more for testing than actual execution:
    -- the performance hit does not matter.
    concatFor (Boxed.convert attrs) $ \(BlockAttribute aid nfacts) ->
      takeFacts ehash eid aid nfacts

concatFor :: Applicative m => Boxed.Vector a -> (a -> m (Boxed.Vector b)) -> m (Boxed.Vector b)
concatFor xs body =
  fmap (Boxed.concatMap id) $ for xs body

takeFacts ::
  EntityHash ->
  EntityId ->
  AttributeId ->
  Int ->
  EitherT FactError (State ValueState) (Boxed.Vector Fact)
takeFacts ehash eid aid nfacts = do
  ixs <- Boxed.convert <$> takeIndices nfacts
  vs <- takeValues aid nfacts
  pure $
    Boxed.zipWith (mkFact ehash eid aid) ixs vs

mkFact :: EntityHash -> EntityId -> AttributeId -> BlockIndex -> Value -> Fact
mkFact ehash eid aid (BlockIndex time priority tombstone) value =
  Fact ehash eid aid time priority $
    case tombstone of
      Tombstone ->
        Nothing'
      NotTombstone ->
        Just' value

takeIndices :: Int -> EitherT FactError (State ValueState) (Unboxed.Vector BlockIndex)
takeIndices n = do
  ValueState is0 vss0 <- get

  let
    (js, is) =
      Unboxed.splitAt n is0

  when (Unboxed.length js /= n) $
    left FactIndicesExhausted

  put $ ValueState is vss0
  pure js

takeValues :: AttributeId -> Int -> EitherT FactError (State ValueState) (Boxed.Vector Value)
takeValues aid@(AttributeId aix) n = do
  ValueState is0 vss0 <- get

  case vss0 Boxed.!? aix of
    Nothing ->
      left $ FactNoValues aid

    Just vs0 -> do
      let
        (us, vs) =
          Boxed.splitAt n vs0

        vss =
          vss0 Boxed.// [(aix, vs)]

      when (Boxed.length us /= n) $
        left $ FactValuesExhausted aid

      put $ ValueState is0 vss
      pure us
