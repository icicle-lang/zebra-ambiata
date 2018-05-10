{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Merge.Mutable (
    UnionTableError(..)
  , renderUnionTableError

  , unionStriped
  , unionStripedWith
  ) where

import           Control.Monad.Morph (hoist)
import           Control.Monad.Primitive (PrimMonad, PrimState)
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.ST (runST)

import qualified Data.Vector as Boxed

import           P

import           Viking (Stream, Of)
import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, left, runEitherT)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Table.Data
import           Zebra.Table.Logical (LogicalMergeError)
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Mutable as Mutable
import           Zebra.Table.Schema (SchemaUnionError, SchemaError)
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped
import           Zebra.X.Either

data OpenInput m =
    OpenInput' {
      inputLogicalKeys :: !(Boxed.Vector Logical.Value)
    , inputDefault     :: !Default
    , inputKeyColumn   :: !Striped.Column
    , inputValueColumn :: !Striped.Column
    , inputStream      :: Stream (Of Striped.Table) (EitherT UnionTableError m) ()
    }

data Input m =
    OpenInput (OpenInput m)
  | ClosedInput

isClosed :: Input m -> Bool
isClosed ClosedInput = True
isClosed _ = False

hasData :: OpenInput m -> Bool
hasData = not . Boxed.null . inputLogicalKeys
{-# INLINABLE hasData #-}

data UnionTableError =
    UnionEmptyInput
  | UnionStripedError !StripedError
  | UnionStripedSchemaError !SchemaError
  | UnionLogicalMergeError !LogicalMergeError
  | UnionSchemaError !SchemaUnionError
  | UnionMutableError !Mutable.MutableError
    deriving (Eq, Show)

renderUnionTableError :: UnionTableError -> Text
renderUnionTableError = \case
  UnionEmptyInput ->
    "Cannot merge empty files"
  UnionStripedError err ->
    Striped.renderStripedError err
  UnionStripedSchemaError err ->
    Schema.renderSchemaError err
  UnionLogicalMergeError err ->
    Logical.renderLogicalMergeError err
  UnionSchemaError err ->
    Schema.renderSchemaUnionError err
  UnionMutableError err ->
    Mutable.renderMutableError err

------------------------------------------------------------------------
-- General

unionSchemas :: Cons Boxed.Vector Schema.Table -> Either UnionTableError Schema.Table
unionSchemas =
  first UnionSchemaError . Cons.fold1M' Schema.union
{-# INLINABLE unionSchemas #-}

peekHead :: Monad m => Stream (Of x) m r -> EitherT UnionTableError m (x, Stream (Of x) m r)
peekHead input = do
  e <- lift $ Stream.next input
  case e of
    Left _r ->
      left UnionEmptyInput
    Right (hd, tl) ->
      pure (hd, Stream.cons hd tl)
{-# INLINABLE peekHead #-}

updateInput ::
     Monad m
  => Input m
  -> EitherT UnionTableError m (Input m)
updateInput input =
  case input of
    ClosedInput ->
      pure ClosedInput
    OpenInput x ->
      if hasData x then
        pure input
      else do
        pullInput (inputStream x)

pullInput ::
     Monad m
  => Stream (Of Striped.Table) (EitherT UnionTableError m) ()
  -> EitherT UnionTableError m (Input m)
pullInput from =
  Stream.next from >>= \case
    Left () ->
      pure ClosedInput

    Right (table, remaining) -> do
      (d,k,v)  <- hoistWith UnionStripedSchemaError
                $ Striped.takeMap table
      -- Get the keys and convert them to Logicals
      keys     <- hoistWith UnionStripedError
                $ Striped.toValues k
      pure $ OpenInput $ OpenInput' keys d k v remaining

unionStep ::
     PrimMonad m
  => Mutable.Table (PrimState m)
  -> Cons Boxed.Vector (Input om)
  -> (EitherT UnionTableError m) (Cons Boxed.Vector (Input om))
unionStep (Mutable.Map _ mk mv) inputs = do
  let
    maximumKey :: Input xx -> Maybe Logical.Value
    maximumKey (OpenInput (OpenInput' lk _ _ _ _))
      | not (Boxed.null lk)
      = Just (Boxed.unsafeLast lk)
    maximumKey _
      = Nothing

    maximums =
      Cons.mapMaybe maximumKey inputs

  if Boxed.null maximums then
    pure inputs
  else do
    let
      key =
        Boxed.minimum maximums

      split = flip fmap inputs $ \case
        ClosedInput -> (Nothing, ClosedInput)
        OpenInput x ->
          let
            (includeLogical, excludeLogical)
              = Boxed.partition (<= key) (inputLogicalKeys x)

            splitLength
              = Boxed.length includeLogical

            def
              = inputDefault x

            (includeKeys, excludeKeys)
              = Striped.splitAtColumn splitLength (inputKeyColumn x)

            (includeValues, excludeValues)
              = Striped.splitAtColumn splitLength (inputValueColumn x)

            tup
              = (includeLogical, def, includeKeys, includeValues)

            continue
              = OpenInput' excludeLogical def excludeKeys excludeValues (inputStream x)

          in
            (Just tup, OpenInput continue)

    let
      (this, next)
        = Cons.unzip split

    firstT UnionMutableError $
      Mutable.mergeMaps mk mv (Cons.unsafeFromVector $ Cons.mapMaybe id this)

    pure next

unionStep _ _ = left UnionEmptyInput


unionInput ::
     Monad m
  => Schema.Table
  -> Cons Boxed.Vector (Input m)
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionInput schema unadjustedInputs = do
  inputs <- lift $ traverse updateInput unadjustedInputs

  if Cons.all isClosed inputs then
    pure ()
  else do
    (newInputs, merged) <- lift . hoistEither $ runST $ runEitherT $ do
      mTable    <- Mutable.empty 256 schema
      newInputs <- unionStep mTable inputs
      merged    <- Mutable.unsafeFreeze mTable
      return (newInputs, merged)

    if (Striped.length merged == 0) then
      unionInput schema newInputs
    else do
      Stream.yield merged
      unionInput schema newInputs

unionStripedWith ::
     Monad m
  => Schema.Table
  -> Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStripedWith schema inputs0 = do
  let
    fromStriped =
      Stream.mapM (hoistEither . first UnionStripedError . Striped.transmute schema) . hoist lift

    tables = fmap fromStriped inputs0

  inputs <- lift $ traverse pullInput tables

  Stream.whenEmpty (Striped.empty schema) $
    unionInput schema inputs
{-# INLINABLE unionStripedWith #-}

unionStriped ::
     Monad m
  => Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStriped inputs0 = do
  (heads, inputs1) <- fmap Cons.unzip . lift $ traverse peekHead inputs0
  schema <- lift . hoistEither . unionSchemas $ fmap Striped.schema heads
  unionStripedWith schema inputs1
{-# INLINABLE unionStriped #-}

