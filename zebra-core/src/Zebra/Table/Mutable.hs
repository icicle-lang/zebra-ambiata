{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Table.Mutable (
  -- * Mutable variants of Striped Tables
  --   and Columns
    Table(..)
  , Column(..)

  -- * Errors
  , MutableError(..)
  , renderMutableError

  -- * Initialisation
  , empty
  , emptyColumn

  -- * Append
  , append
  , appendColumn

  -- * Merge
  , merges
  , mergeMaps
  , mergeColumns

  -- * Freeze
  , freeze
  , freezeColumn
  , unsafeFreeze
  , unsafeFreezeColumn
  ) where

import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Primitive (PrimMonad, PrimState)

import           Data.Word (Word8)
import qualified Data.Map.Lazy as Map
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Semigroup ((<>))
import           Data.Void (absurd)

import           P hiding (empty, concat, splitAt, length, (<>))

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, hoistMaybe, left)
import qualified X.Data.Vector as Boxed
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons
import           X.Data.Vector.Grow (Grow)
import qualified X.Data.Vector.Grow as Grow
import           X.Data.Vector.Ref (Ref)
import qualified X.Data.Vector.Ref as Ref

import           Zebra.Table.Data
import qualified Zebra.Table.Encoding as Encoding
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Striped as Striped
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.X.Vector.Storable as Storable
import           Zebra.X.Either (hoistWith)

data Table s =
    Binary !Default !Encoding.Binary !(Grow Storable.MVector s Word8)
  | Array !Default !(Column s)
  | Map !Default !(Column s) !(Column s)

data Column s =
    Unit (Ref Storable.MVector s Int)
  | Int !Default !Encoding.Int !(Grow Storable.MVector s Int64)
  | Double !Default !(Grow Storable.MVector s Double)
  | Enum !Default !(Grow Storable.MVector s Tag) !(Cons Boxed.Vector (Variant (Column s)))
  | Struct !Default !(Cons Boxed.Vector (Field (Column s)))
  | Nested !(Grow Storable.MVector s Int64) !(Table s)
  | Reversed !(Column s)

data MutableError =
    MutableError
  | MutableStripedError Striped.StripedError
  | MutableSchemaError Schema.SchemaError
  | MutableLogicalMergeError
  deriving (Eq, Show)

renderMutableError :: MutableError -> Text
renderMutableError _ = ""
------------------------------------------------------------------------
-- Initialisation

empty :: PrimMonad m => Int -> Schema.Table -> m (Table (PrimState m))
empty capacity = \case
  Schema.Binary def enc ->
    Binary def enc <$> Grow.new capacity

  Schema.Array def col ->
    Array def <$> emptyColumn capacity col

  Schema.Map def k v ->
    Map def <$> emptyColumn capacity k <*> emptyColumn capacity v

emptyColumn :: PrimMonad m => Int -> Schema.Column -> m (Column (PrimState m))
emptyColumn capacity = \case
  Schema.Unit ->
    Unit <$> Ref.newRef 0

  Schema.Int def enc ->
    Int def enc <$> Grow.new capacity

  Schema.Double def ->
    Double def <$> Grow.new capacity

  Schema.Enum def variants ->
    Enum def <$> Grow.new capacity <*> traverse (traverse (emptyColumn capacity)) variants

  Schema.Struct def fields ->
    Struct def <$> traverse (traverse (emptyColumn capacity)) fields

  Schema.Nested table ->
    Nested <$> Grow.new capacity <*> empty capacity table

  Schema.Reversed column ->
    Reversed <$> emptyColumn capacity column

------------------------------------------------------------------------
-- Length

length :: PrimMonad m => Table (PrimState m) -> m Int
length = \case
  Binary _ _ bs
    -> Grow.length bs

  Array _ a
    -> lengthColumn a

  Map _ k _
    -> lengthColumn k

lengthColumn :: PrimMonad m => Column (PrimState m) -> m Int
lengthColumn = \case
  Unit r ->
    Ref.readRef r

  Int _ _ is ->
    Grow.length is

  Double _ ds ->
    Grow.length ds

  Enum _ ts _ ->
    Grow.length ts

  Struct _ fs ->
    lengthColumn . fieldData $ Cons.head fs

  Nested ns _ -> do
    Grow.length ns

  Reversed rs -> do
    lengthColumn rs

------------------------------------------------------------------------
-- Appending

append :: PrimMonad m => Table (PrimState m) -> Striped.Table -> EitherT MutableError m ()
append x0 x1 =
  case (x0, x1) of
    (Binary def0 encoding0 bs0, Striped.Binary def1 encoding1 bs1)
      | def0 == def1
      , encoding0 == encoding1
      ->
        lift $ Grow.append bs0 (Storable.unsafeFromByteString bs1)

    (Array def0 a0, Striped.Array def1 a1)
      | def0 == def1
      ->
        appendColumn a0 a1

    (Map def0 k0 v0, Striped.Map def1 k1 v1)
      | def0 == def1
      -> do
        appendColumn k0 k1
        appendColumn v0 v1

    _ ->
      hoistEither . Left $ MutableError

appendColumn :: PrimMonad m => Column (PrimState m) -> Striped.Column -> EitherT MutableError m ()
appendColumn x0 x1 =
  case (x0, x1) of
    (Unit n0, Striped.Unit n1) ->
      lift $ Ref.modifyRef n0 (+ n1)

    (Int def0 encoding0 xs0, Striped.Int def1 encoding1 xs1)
      | def0 == def1
      , encoding0 == encoding1
      ->
        lift $ Grow.append xs0 xs1

    (Double def0 xs0, Striped.Double def1 xs1)
      | def0 == def1
      ->
        lift $ Grow.append xs0 xs1

    (Enum def0 tags0 vs0, Striped.Enum def1 tags1 vs1)
      | def0 == def1
      , Cons.length vs0 == Cons.length vs1
      -> do lift $ Grow.append tags0 tags1
            -- TODO: Check variant names
            _   <- Cons.zipWithM (\a b -> appendColumn (variantData a) (variantData b)) vs0 vs1
            return ()

    (Struct def0 vs0, Striped.Struct def1 vs1)
      | def0 == def1
      , Cons.length vs0 == Cons.length vs1
      -> do -- TODO: Check field names
            _   <- Cons.zipWithM (\a b -> appendColumn (fieldData a) (fieldData b)) vs0 vs1
            return ()

    (Nested ns0 t0, Striped.Nested ns1 t1)
      -> do
        lift $ Grow.append ns0 ns1
        append t0 t1

    (Reversed c0, Striped.Reversed c1) ->
      appendColumn c0 c1

    _ ->
      hoistEither . Left $ MutableError


------------------------------------------------------------------------
-- Merge

data Step =
    StrideOn Int (Int, Int)
  | MergeOf (NonEmpty (Int, Int))

-- | This function is of poor quality.
planMerges :: Cons Boxed.Vector (Boxed.Vector Logical.Value) -> Either x [Step]
planMerges xxs = do
  tagged <- cimapM (\stream -> ximapM (\index x -> pure (x, (stream, index) :| []))) xxs
  let
    expanded =
      Map.toList . Map.unionsWith (<>) . fmap (Map.fromAscList . toList) . toList $ tagged

    collapsed =
      foldr go [] expanded

  pure collapsed

  where
    go (_, (stream, index) :| []) acc
       | StrideOn nextStream (ifrom, run) : rest <- acc
       , stream == nextStream && index == ifrom - 1
       = StrideOn stream (index, run + 1) : rest
       | otherwise
       = StrideOn stream (index, 1) : acc
    go (_, xs) acc
       = MergeOf xs : acc


merges :: PrimMonad m => Table (PrimState m) -> Cons Boxed.Vector Striped.Table -> EitherT MutableError m ()
merges mt ts = case mt of
  Binary _ _ _ ->
    foldM_ (const $ append mt) () ts

  Array _ _ ->
    foldM_ (const $ append mt) () ts

  Map _ mk mv -> do
    -- Maps are the interesting case.
    --
    -- We only have Ord for Logical Values, so we
    -- need to convert the keys into Logical before
    -- we start to churn through.
    maps     <- hoistWith MutableSchemaError
              $ traverse Striped.takeMap ts

    -- Get the keys and convert them to Logicals
    keys     <- hoistWith MutableStripedError
              $ traverse (\(_,k,_) -> Striped.toValues k) maps

    let to = Cons.zipWith (\p (d,k,v) -> (p,d,k,v)) keys maps

    -- Run the mutable merge
    mergeMaps mk mv to

mergeMaps :: PrimMonad m
          => Column (PrimState m) -- ^ Mutable map of keys
          -> Column (PrimState m) -- ^ Mutable map of values
          -> Cons Boxed.Vector (Boxed.Vector Logical.Value, Default, Striped.Column, Striped.Column) -- ^ Striped Maps
          -> EitherT MutableError m ()
mergeMaps mk mv maps = do
    -- The striped values stay as they are.
    -- We can slice out what we need.
  let
    pkeys =
      fmap (\(pk,_,_,_) -> pk) maps

    keys' =
      fmap (\(_,_,k,_) -> k) maps

    values' =
      fmap (\(_,_,_,v) -> v) maps

  plan     <- hoistWith absurd $ planMerges pkeys

  for_ plan $ \case
    StrideOn index (from, run) -> do
      k <- hoistMaybe MutableError $ Cons.index index keys'
      v <- hoistMaybe MutableError $ Cons.index index values'
      appendColumn mk $ Striped.sliceColumn from run k
      appendColumn mv $ Striped.sliceColumn from run v

    MergeOf xs@((headIndex,headRow) :| _) -> do
      k  <- hoistMaybe MutableError $ Cons.index headIndex keys'
      vs <- hoistMaybe MutableError $ for xs $ \(i,f) -> Striped.sliceColumn f 1 <$> Cons.index i values'
      appendColumn mk $ Striped.sliceColumn headRow 1 k
      mergeColumns mv $ Cons.fromNonEmpty vs


mergeColumns :: PrimMonad m => Column (PrimState m) -> Cons Boxed.Vector Striped.Column -> EitherT MutableError m ()
mergeColumns mc cs =
  case mc of
    Unit ref -> do
      _ <- hoistWith MutableSchemaError $ for cs Striped.takeUnit
      Ref.modifyRef ref (+1)

    Int _ _ _ ->
      left MutableLogicalMergeError

    Double _ _ ->
      left MutableLogicalMergeError

    Enum _ _ _ ->
      left MutableLogicalMergeError

    Struct _ fs -> do
      structs <- hoistWith MutableSchemaError $ for cs Striped.takeStruct
      void . Cons.iforM fs $ \i f -> do
        toMergeOn <- hoistMaybe MutableError $ for structs (fmap fieldData . Cons.index i . snd)
        mergeColumns (fieldData f) toMergeOn

    Nested n t -> do
      -- Invariant:
      -- Each of the nesting descriptions should
      -- have only one value (it's on value to)
      -- merge
      nestings <- hoistWith MutableSchemaError $ for cs Striped.takeNested
      sLen     <- length t
      merges t (fmap snd nestings)
      fLen     <- length t
      Grow.add n (fromIntegral $ fLen - sLen)

    Reversed rs -> do
      revs <- hoistWith MutableSchemaError $ for cs Striped.takeReversed
      mergeColumns rs revs

------------------------------------------------------------------------
-- Freeze

freeze :: PrimMonad m => Table (PrimState m) -> m Striped.Table
freeze = \case
  Binary def encoding bs
    -> Striped.Binary def encoding . Storable.unsafeToByteString <$> Grow.freeze bs

  Array def a
    -> Striped.Array def <$> freezeColumn a

  Map def k v
    -> Striped.Map def <$> freezeColumn k <*> freezeColumn v

freezeColumn :: PrimMonad m => Column (PrimState m) -> m Striped.Column
freezeColumn = \case
  Unit n
    -> Striped.Unit <$> Ref.readRef n

  Int def encoding xs
    -> Striped.Int def encoding <$> Grow.freeze xs

  Double def xs
    -> Striped.Double def <$> Grow.freeze xs

  Enum def tags vs
    -> Striped.Enum def <$> Grow.freeze tags <*> traverse (traverse freezeColumn) vs

  Struct def vs
    -> Striped.Struct def <$> traverse (traverse freezeColumn) vs

  Nested ns t
    -> Striped.Nested <$> Grow.freeze ns <*> freeze t

  Reversed c
    -> Striped.Reversed <$> freezeColumn c


unsafeFreeze :: PrimMonad m => Table (PrimState m) -> m Striped.Table
unsafeFreeze = \case
  Binary def encoding bs
    -> Striped.Binary def encoding . Storable.unsafeToByteString <$> Grow.unsafeFreeze bs

  Array def a
    -> Striped.Array def <$> unsafeFreezeColumn a

  Map def k v
    -> Striped.Map def <$> unsafeFreezeColumn k <*> unsafeFreezeColumn v

unsafeFreezeColumn :: PrimMonad m => Column (PrimState m) -> m Striped.Column
unsafeFreezeColumn = \case
  Unit n
    -> Striped.Unit <$> Ref.readRef n

  Int def encoding xs
    -> Striped.Int def encoding <$> Grow.unsafeFreeze xs

  Double def xs
    -> Striped.Double def <$> Grow.unsafeFreeze xs

  Enum def tags vs
    -> Striped.Enum def <$> Grow.unsafeFreeze tags <*> traverse (traverse unsafeFreezeColumn) vs

  Struct def vs
    -> Striped.Struct def <$> traverse (traverse unsafeFreezeColumn) vs

  Nested ns t
    -> Striped.Nested <$> Grow.unsafeFreeze ns <*> unsafeFreeze t

  Reversed c
    -> Striped.Reversed <$> unsafeFreezeColumn c

