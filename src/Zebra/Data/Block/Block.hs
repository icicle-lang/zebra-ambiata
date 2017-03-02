{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Block.Block (
    Block(..)

  , FactError(..)
  , blockOfFacts
  , factsOfBlock

  , EntityError(..)
  , entitiesOfBlock
  ) where

import           Control.Monad.Primitive (PrimMonad(..))
import           Control.Monad.ST (runST)
import           Control.Monad.State.Strict (MonadState(..))
import           Control.Monad.Trans.State.Strict (State, runState)

import           Data.Typeable (Typeable)
import qualified Data.Vector.Mutable as MBoxed

import           GHC.Generics (Generic)

import           P

import           X.Control.Monad.Trans.Either (EitherT, runEitherT, left, hoistEither)
import qualified X.Data.Vector as Boxed
import           X.Data.Vector.Ref (Ref, newRef, readRef, writeRef)
import qualified X.Data.Vector.Storable as Storable
import qualified X.Data.Vector.Unboxed as Unboxed

import           Zebra.Data.Block.Entity
import           Zebra.Data.Block.Index
import           Zebra.Data.Core
import           Zebra.Data.Entity
import           Zebra.Data.Fact
import           Zebra.Schema (Schema)
import           Zebra.Table (Table, ValueError)
import qualified Zebra.Table as Table
import           Zebra.Table.Mutable (MutableError)
import qualified Zebra.Table.Mutable as MTable
import           Zebra.Value (Value)


data Block a =
  Block {
      blockEntities :: !(Boxed.Vector BlockEntity)
    , blockIndices :: !(Unboxed.Vector BlockIndex)
    , blockTables :: !(Boxed.Vector (Table a))
    } deriving (Eq, Ord, Show, Generic, Typeable, Functor, Foldable, Traversable)

------------------------------------------------------------------------
-- Conversion to/from facts

data FactError a =
    FactValueError !(ValueError a)
  | FactIndicesExhausted
  | FactValuesExhausted !AttributeId
  | FactNoValues !AttributeId
  | FactLeftoverIndices !(Unboxed.Vector BlockIndex)
  | FactLeftoverValues !(Boxed.Vector (Boxed.Vector Value))
    deriving (Eq, Ord, Show, Generic, Typeable)

blockOfFacts :: Boxed.Vector Schema -> Boxed.Vector Fact -> Either MutableError (Block Schema)
blockOfFacts schemas facts =
  Block (entitiesOfFacts facts) (indicesOfFacts facts) <$> MTable.fromFacts schemas facts

factsOfBlock :: Boxed.Vector Schema -> Block a -> Either (FactError a) (Boxed.Vector Fact)
factsOfBlock schemas block = do
  let
    entities =
      blockEntities block
    indices =
      blockIndices block
    tables =
      blockTables block

  values <- first FactValueError $ Boxed.zipWithM Table.rows schemas tables

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

takeEntityFacts :: Boxed.Vector BlockEntity -> EitherT (FactError a) (State ValueState) (Boxed.Vector Fact)
takeEntityFacts entities =
  concatFor entities $ \(BlockEntity ehash eid attrs) ->
    -- The conversion from unboxed to boxed is not ideal here, but this
    -- function is more for testing than actual execution:
    -- the performance hit does not matter.
    concatFor (Boxed.convert attrs) $ \(BlockAttribute aid nfacts) ->
      takeFacts ehash eid aid (fromIntegral nfacts)

concatFor :: Applicative m => Boxed.Vector a -> (a -> m (Boxed.Vector b)) -> m (Boxed.Vector b)
concatFor xs body =
  fmap (Boxed.concatMap id) $ for xs body

takeFacts ::
  EntityHash ->
  EntityId ->
  AttributeId ->
  Int ->
  EitherT (FactError a) (State ValueState) (Boxed.Vector Fact)
takeFacts ehash eid aid nfacts = do
  ixs <- Boxed.convert <$> takeIndices nfacts
  vs <- takeValues aid nfacts
  pure $
    Boxed.zipWith (mkFact ehash eid aid) ixs vs

mkFact :: EntityHash -> EntityId -> AttributeId -> BlockIndex -> Value -> Fact
mkFact ehash eid aid (BlockIndex time factsetId tombstone) value =
  Fact ehash eid aid time factsetId $
    case tombstone of
      Tombstone ->
        Nothing'
      NotTombstone ->
        Just' value

takeIndices :: Int -> EitherT (FactError a) (State ValueState) (Unboxed.Vector BlockIndex)
takeIndices n = do
  ValueState is0 vss0 <- get

  let
    (js, is) =
      Unboxed.splitAt n is0

  when (Unboxed.length js /= n) $
    left FactIndicesExhausted

  put $ ValueState is vss0
  pure js

takeValues :: AttributeId -> Int -> EitherT (FactError a) (State ValueState) (Boxed.Vector Value)
takeValues aid@(AttributeId aix) n = do
  ValueState is0 vss0 <- get

  case vss0 Boxed.!? fromIntegral aix of
    Nothing ->
      left $ FactNoValues aid

    Just vs0 -> do
      let
        (us, vs) =
          Boxed.splitAt n vs0

        vss =
          vss0 Boxed.// [(fromIntegral aix, vs)]

      when (Boxed.length us /= n) $
        left $ FactValuesExhausted aid

      put $ ValueState is0 vss
      pure us

------------------------------------------------------------------------
-- Conversion to/from individual entities

data IndexedEntity =
    IndexedEntity !BlockEntity !(Unboxed.Vector BlockIndex)

data EntityError =
    EntityAttributeNotFound !AttributeId
  | EntityNotEnoughRows
    deriving (Eq, Ord, Show, Generic, Typeable)

entitiesOfBlock :: Block a -> Either EntityError (Boxed.Vector (Entity a))
entitiesOfBlock (Block entities indices tables) =
  runST $ runEitherT $ do
    mtables <- Boxed.thaw tables
    ientities <- hoistEither $ takeIndexedEntities entities indices
    Boxed.mapM (fromBlockEntity mtables) ientities

takeIndexedEntities :: Boxed.Vector BlockEntity -> Unboxed.Vector BlockIndex -> Either EntityError (Boxed.Vector IndexedEntity)
takeIndexedEntities entities indices = do
  let
    ecounts :: Boxed.Vector Int
    ecounts =
      fmap rowsOfEntity entities

    eoffsets :: Boxed.Vector Int
    eoffsets =
      Boxed.prescanl' (+) 0 ecounts

    takeIndex :: Int -> Int -> Either EntityError (Unboxed.Vector BlockIndex)
    takeIndex off len =
      if off + len <= Unboxed.length indices then
        pure $ Unboxed.slice off len indices
      else
        Left EntityNotEnoughRows

  eindices <- Boxed.zipWithM takeIndex eoffsets ecounts
  pure $ Boxed.zipWith IndexedEntity entities eindices

rowsOfEntity :: BlockEntity -> Int
rowsOfEntity (BlockEntity _ _ attrs) =
  fromIntegral . Unboxed.sum $ Unboxed.map attributeRows attrs

fromBlockEntity ::
  PrimMonad m =>
  MBoxed.MVector (PrimState m) (Table a) ->
  IndexedEntity ->
  EitherT EntityError m (Entity a)
fromBlockEntity mtables (IndexedEntity (BlockEntity hash eid battrs) indices) = do
  mindices <- newRef indices
  let battrs' = denseBlockAttributes (MBoxed.length mtables) battrs
  attrs <- Boxed.mapM (fromBlockAttribute mindices mtables) $ Unboxed.convert battrs'
  pure $ Entity hash eid attrs

denseBlockAttributes ::
  Int ->
  Unboxed.Vector BlockAttribute ->
  Unboxed.Vector BlockAttribute
denseBlockAttributes num blockAttributes =
  Unboxed.mapAccumulate go blockAttributes $ Unboxed.enumFromN 0 num
 where
  go battrs ix
   | Just (BlockAttribute aid n, battrs') <- Unboxed.uncons battrs
   , aid == AttributeId ix
   = (battrs', BlockAttribute aid n)
   | otherwise
   = (battrs, BlockAttribute (AttributeId ix) 0)

fromBlockAttribute ::
  PrimMonad m =>
  Ref MBoxed.MVector (PrimState m) (Unboxed.Vector BlockIndex) ->
  MBoxed.MVector (PrimState m) (Table a) ->
  BlockAttribute ->
  EitherT EntityError m (Attribute a)
fromBlockAttribute mindices mtables (BlockAttribute aid n) = do
  indices <- takeIndexRows mindices $ fromIntegral n
  table <- takeTableRows mtables aid $ fromIntegral n

  pure $ Attribute
    (Storable.convert $ Unboxed.map indexTime indices)
    (Storable.convert $ Unboxed.map indexFactsetId indices)
    (Storable.convert $ Unboxed.map indexTombstone indices)
    table

takeIndexRows ::
  PrimMonad m =>
  Ref MBoxed.MVector (PrimState m) (Unboxed.Vector BlockIndex) ->
  Int ->
  m (Unboxed.Vector BlockIndex)
takeIndexRows ref n = do
  indices <- readRef ref

  let
    (indices1, indices2) =
      Unboxed.splitAt (fromIntegral n) indices

  writeRef ref indices2
  pure indices1

takeTableRows ::
  PrimMonad m =>
  MBoxed.MVector (PrimState m) (Table a) ->
  AttributeId ->
  Int ->
  EitherT EntityError m (Table a)
takeTableRows attrs aid@(AttributeId aix0) n =
  let
    aix =
      fromIntegral aix0
  in
    if aix < 0 || aix >= MBoxed.length attrs then
      left $ EntityAttributeNotFound aid
    else do
      table <- MBoxed.unsafeRead attrs aix

      let
        (table1, table2) =
          Table.splitAt n table

      MBoxed.unsafeWrite attrs aix table2

      pure table1
