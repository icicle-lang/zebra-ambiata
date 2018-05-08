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

  , Monoidal(..)
  , valueMonoid
  , measureMonoid
  , summationMonoid

  , Extract(..)
  , identityExtraction
  , minimumExtraction
  ) where

import           Control.Monad.Morph (hoist, squash)
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.State.Strict (StateT, runStateT, modify)

import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as Boxed

import           P

import           Viking (Stream, Of)
import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, left)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Table.Data
import qualified Zebra.Table.Encoding as Encoding
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

data Input m a =
  Input {
      inputData :: !(Map Logical.Value a)
    , inputStream :: !(Maybe (Stream (Of Logical.Table) m ()))
    }

data Step m a =
  Step {
      _stepComplete :: !(Map Logical.Value a)
    , _stepRemaining :: !(Cons Boxed.Vector (Input m a))
    }

data Extract a =
  Extract {
      extractResult :: Map Logical.Value a -> Map Logical.Value a
    }

identityExtraction :: Extract a
identityExtraction =
  Extract id

minimumExtraction :: Int64 -> Extract Int64
minimumExtraction x =
  Extract $ Map.filter (> x)

data Monoidal a =
  Monoidal {
      monoidFromValue :: Logical.Value -> a
    , monoidToValue :: a -> Logical.Value
    , monoidSchema :: Schema.Table -> Either UnionTableError Schema.Table
    , monoidOp :: a -> a -> Either LogicalMergeError a
    }

valueMonoid :: Monoidal Logical.Value
valueMonoid =
  Monoidal id id (pure . id) Logical.mergeValue

measureMonoid :: Monoidal Int64
measureMonoid =
  Monoidal Logical.sizeValue Logical.Int
    (\schema ->
       case schema of
         Schema.Map def k _ ->
           pure $ Schema.Map def k (Schema.Int DenyDefault Encoding.Int)
         t0 ->
           Left $ UnionTableMonoidNotDefined t0
    )
    (\x0 x1 -> pure $ x0 + x1)

summationMonoid :: Monoidal Logical.Value
summationMonoid =
  Monoidal id id (pure . id) Logical.sumValue

data UnionTableError =
    UnionEmptyInput
  | UnionStripedError !StripedError
  | UnionLogicalSchemaError !LogicalSchemaError
  | UnionLogicalMergeError !LogicalMergeError
  | UnionSchemaError !SchemaUnionError
  | UnionTableMonoidNotDefined !Schema.Table
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
  UnionTableMonoidNotDefined _ ->
    "Monoid not defined for schema of this shape."

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

hasData :: Input m a -> Bool
hasData =
  not . Map.null . inputData
{-# INLINABLE hasData #-}

replaceData :: Map Logical.Value a -> Input m a -> Input m a
replaceData values input =
  input {
      inputData =
        values
    }
{-# INLINABLE replaceData #-}

dropData :: Map Logical.Value b -> Input m a -> Input m a
dropData drops input =
  input {
      inputData =
        inputData input `Map.difference` drops
    }
{-# INLINABLE dropData #-}

replaceStream :: Stream (Of Logical.Table) m () ->  Input m a -> Input m a
replaceStream stream input =
  input {
      inputStream =
        Just stream
    }
{-# INLINABLE replaceStream #-}

isClosed :: Input m a -> Bool
isClosed =
  isNothing . inputStream
{-# INLINABLE isClosed #-}

closeStream :: Input m a -> Input m a
closeStream input =
  input {
      inputStream =
        Nothing
    }
{-# INLINABLE closeStream #-}

updateInput ::
     Monad m
  => (Logical.Value -> a)
  -> Input m a
  -> StateT (Map Logical.Value Int64) (EitherT UnionTableError m) (Input m a)
updateInput f input =
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

          Right (hd, tl) -> do
            values <- lift . firstT UnionLogicalSchemaError . hoistEither $ Logical.takeMap hd
            modify $ Map.unionWith (+) (Map.map Logical.sizeValue values)
            pure . replaceStream tl $ replaceData (fmap f values) input
{-# INLINABLE updateInput #-}

takeExcessiveValues :: Maybe MaximumRowSize -> Map Logical.Value Int64 -> Map Logical.Value Int64
takeExcessiveValues = \case
  Nothing ->
    const Map.empty
  Just size ->
    Map.filter (> unMaximumRowSize size)
{-# INLINABLE takeExcessiveValues #-}

unionStep ::
     Monad m
  => (a -> a -> Either LogicalMergeError a)
  -> (Map Logical.Value a -> Map Logical.Value a)
  -> Cons Boxed.Vector (Input m a)
  -> EitherT UnionTableError m (Step m a)
unionStep f g inputs = do
  step <- firstT UnionLogicalMergeError . hoistEither . Logical.unionStep f $ fmap inputData inputs
  pure $
    Step
      (g $ Logical.unionComplete step)
      (Cons.zipWith replaceData(Logical.unionRemaining step) inputs)
{-# INLINABLE unionStep #-}

unionInput ::
     Monad m
  => Monoidal a
  -> Extract a
  -> Maybe MaximumRowSize
  -> Cons Boxed.Vector (Input m a)
  -> Map Logical.Value Int64
  -> Stream (Of Logical.Table) (EitherT UnionTableError m) ()
unionInput m e msize inputs0 sizes0 = do
  (inputs1, sizes1) <- lift $ runStateT (traverse (updateInput (monoidFromValue m)) inputs0) sizes0

  let
    drops =
      takeExcessiveValues msize sizes1

    inputs2 =
      fmap (dropData drops) inputs1

  if Cons.all isClosed inputs2 then do
    pure ()
  else do
    Step values inputs3 <- lift $ unionStep (monoidOp m) (extractResult e) inputs2

    if Map.null values then
      unionInput m e msize inputs3 sizes1
    else do
      Stream.yield . Logical.Map . fmap (monoidToValue m) $ values
      unionInput m e msize inputs3 sizes1
{-# INLINABLE unionInput #-}

unionLogical ::
     Monad m
  => Monoidal a
  -> Extract a
  -> Schema.Table
  -> Maybe MaximumRowSize
  -> Cons Boxed.Vector (Stream (Of Logical.Table) m ())
  -> Stream (Of Logical.Table) (EitherT UnionTableError m) ()
unionLogical m e schema msize inputs = do
  Stream.whenEmpty (Logical.empty schema) $
    unionInput m e msize (fmap (Input Map.empty . Just) inputs) Map.empty
{-# INLINABLE unionLogical #-}

unionStripedWith ::
     Monad m
  => Monoidal a
  -> Extract a
  -> Schema.Table
  -> Maybe MaximumRowSize
  -> Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStripedWith m e schema0 msize inputs0 = do
  let
    fromStriped =
      Stream.mapM (hoistEither . first UnionStripedError . Striped.toLogical) .
      Stream.mapM (hoistEither . first UnionStripedError . Striped.transmute schema0) .
      hoist lift

  schema1 <- lift . hoistEither $ monoidSchema m schema0

  hoist squash .
    Stream.mapM (hoistEither . first UnionStripedError . Striped.fromLogical schema1) $
    unionLogical m e schema0 msize (fmap fromStriped inputs0)
{-# INLINABLE unionStripedWith #-}

unionStriped ::
     Monad m
  => Monoidal a
  -> Extract a
  -> Maybe MaximumRowSize
  -> Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStriped m e msize inputs0 = do
  (heads, inputs1) <- fmap Cons.unzip . lift $ traverse peekHead inputs0
  schema <- lift . hoistEither . unionSchemas $ fmap Striped.schema heads
  unionStripedWith m e schema msize inputs1
{-# INLINABLE unionStriped #-}
