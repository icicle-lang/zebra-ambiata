{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Merge.Table (
    MaximumRowSize(..)

  , UnionTableError(..)
  , renderUnionTableError

  , unionLogical
  , unionStriped
  , unionStripedWith
  ) where

import           Control.Monad.Morph (hoist, squash)
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.State.Strict (StateT, runStateT, modify')

import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as Boxed

import           P

import           Viking (Stream, Of)
import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, left)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Table.Logical (LogicalSchemaError, LogicalMergeError)
import qualified Zebra.Table.Logical as Logical
import           Zebra.Table.Schema (SchemaUnionError)
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped


newtype MaximumRowSize =
  MaximumRowSize {
      unMaximumRowSize :: Int64
    } deriving (Eq, Ord, Show)

data Input m =
  Input {
      inputData :: !(Map Logical.Value Logical.Value)
    , inputStream :: !(Maybe (Stream (Of Logical.Table) m ()))
    }

data Step m =
  Step {
      _stepComplete :: !(Map Logical.Value Logical.Value)
    , _stepRemaining :: !(Cons Boxed.Vector (Input m))
    }

data UnionTableError =
    UnionEmptyInput
  | UnionStripedError !StripedError
  | UnionLogicalSchemaError !LogicalSchemaError
  | UnionLogicalMergeError !LogicalMergeError
  | UnionSchemaError !SchemaUnionError
    deriving (Eq, Show)

renderUnionTableError :: UnionTableError -> Text
renderUnionTableError = \case
  UnionEmptyInput ->
    "Cannot merge empty files"
  UnionStripedError err ->
    Striped.renderStripedError err
  UnionLogicalSchemaError err ->
    Logical.renderLogicalSchemaError err
  UnionLogicalMergeError err ->
    Logical.renderLogicalMergeError err
  UnionSchemaError err ->
    Schema.renderSchemaUnionError err

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

hasData :: Input m -> Bool
hasData =
  not . Map.null . inputData
{-# INLINABLE hasData #-}

replaceData :: Map Logical.Value Logical.Value -> Input m -> Input m
replaceData values input =
  input {
      inputData =
        values
    }
{-# INLINABLE replaceData #-}

dropData :: Map Logical.Value a -> Input m -> Input m
dropData drops input =
  input {
      inputData =
        inputData input `Map.difference` drops
    }
{-# INLINABLE dropData #-}

isClosed :: Input m -> Bool
isClosed =
  isNothing . inputStream
{-# INLINABLE isClosed #-}

closeStream :: Input m -> Input m
closeStream input =
  input {
      inputStream =
        Nothing
    }
{-# INLINABLE closeStream #-}

updateInput ::
     Monad m
  => Input m
  -> StateT (Map Logical.Value Int64) (EitherT UnionTableError m) (Input m)
updateInput input =
  case inputStream input of
    Nothing ->
      pure input
    Just stream ->
      if hasData input then
        pure input
      else do
        e <- lift . lift $ Stream.next stream
        case e of
          Left () ->
            pure $
              closeStream input

          Right (table, remaining) -> do
            values <- lift . firstT UnionLogicalSchemaError . hoistEither $ Logical.takeMap table
            modify' $ Map.unionWith (+) (Map.map Logical.sizeValue values)
            pure $ Input values (Just remaining)
{-# INLINABLE updateInput #-}

takeExcessiveValues :: Maybe MaximumRowSize -> Map Logical.Value Int64 -> Map Logical.Value Int64
takeExcessiveValues = \case
  Nothing ->
    const Map.empty
  Just size ->
    Map.filter (> unMaximumRowSize size)
{-# INLINABLE takeExcessiveValues #-}

unionStep :: Monad m => Logical.Value -> Cons Boxed.Vector (Input m) -> EitherT UnionTableError m (Step m)
unionStep key inputs = do
  step <- firstT UnionLogicalMergeError . hoistEither . (Logical.unionStep key) $ fmap inputData inputs
  pure $
    Step
      (Logical.unionComplete step)
      (Cons.zipWith replaceData (Logical.unionRemaining step) inputs)
{-# INLINABLE unionStep #-}

maximumKey :: Map Logical.Value Logical.Value -> Maybe Logical.Value
maximumKey kvs =
  if Map.null kvs then
    Nothing
  else
    pure . fst $ Map.findMax kvs
{-# INLINABLE maximumKey #-}

unionInput ::
     Monad m
  => Maybe MaximumRowSize
  -> Cons Boxed.Vector (Input m)
  -> Map Logical.Value Int64
  -> Stream (Of Logical.Table) (EitherT UnionTableError m) ()
unionInput msize inputs0 sizes0 = do
  (inputs1, sizes1) <- lift $ runStateT (traverse updateInput inputs0) sizes0
  unless (Cons.all isClosed inputs1) $ do
    let
      drops =
        takeExcessiveValues msize sizes1

      inputs2 =
        fmap (dropData drops) inputs1

      maximums =
        Cons.mapMaybe (maximumKey . inputData) inputs1

    if Boxed.null maximums then
      unionInput msize inputs2 sizes1
    else do
      Step values inputs3 <- lift $ unionStep (Boxed.minimum maximums) inputs2
      let
        unyieldedSizes
          = sizes1 `Map.difference` values

      Stream.yield $ Logical.Map values
      unionInput msize inputs3 unyieldedSizes
{-# INLINABLE unionInput #-}

unionLogical ::
     Monad m
  => Schema.Table
  -> Maybe MaximumRowSize
  -> Cons Boxed.Vector (Stream (Of Logical.Table) m ())
  -> Stream (Of Logical.Table) (EitherT UnionTableError m) ()
unionLogical schema msize inputs = do
  Stream.whenEmpty (Logical.empty schema) $
    unionInput msize (fmap (Input Map.empty . Just) inputs) Map.empty
{-# INLINABLE unionLogical #-}

unionStripedWith ::
     Monad m
  => Schema.Table
  -> Maybe MaximumRowSize
  -> Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStripedWith schema msize inputs0 = do
  let
    fromStriped =
      Stream.mapM (hoistEither . first UnionStripedError . Striped.toLogical) .
      Stream.mapM (hoistEither . first UnionStripedError . Striped.transmute schema) .
      hoist lift

  hoist squash .
    Stream.mapM (hoistEither . first UnionStripedError . Striped.fromLogical schema) $
    unionLogical schema msize (fmap fromStriped inputs0)
{-# INLINABLE unionStripedWith #-}

unionStriped ::
     Monad m
  => Maybe MaximumRowSize
  -> Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStriped msize inputs0 = do
  (heads, inputs1) <- fmap Cons.unzip . lift $ traverse peekHead inputs0
  schema <- lift . hoistEither . unionSchemas $ fmap Striped.schema heads
  unionStripedWith schema msize inputs1
{-# INLINABLE unionStriped #-}
