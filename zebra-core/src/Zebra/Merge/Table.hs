{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Merge.Table (
    UnionTableError(..)

  , unionLogical
  , unionStriped
  ) where

import           Control.Monad.Morph (hoist, squash)
import           Control.Monad.Trans.Class (lift)

import qualified Data.List as List
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as Boxed

import           GHC.Types (SPEC(..))

import           P

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, left)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Stream (Stream, Of)
import qualified Zebra.Stream as Stream
import           Zebra.Table.Logical (LogicalSchemaError, LogicalMergeError)
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped

data UnionTableError =
    UnionEmptyInput
  | UnionStripedError !StripedError
  | UnionLogicalSchemaError !LogicalSchemaError
  | UnionLogicalMergeError !LogicalMergeError
  | UnionSchemaMismatch !Schema.Table !Schema.Table
    deriving (Eq, Show)

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

------------------------------------------------------------------------
-- General

takeSchema :: Cons Boxed.Vector Schema.Table -> Either UnionTableError Schema.Table
takeSchema inputs =
  let
    (schema0, schemas) =
      Cons.uncons inputs
  in
    case Boxed.find (/= schema0) schemas of
      Nothing ->
        pure schema0
      Just wrong ->
        Left $ UnionSchemaMismatch schema0 wrong
{-# INLINABLE takeSchema #-}

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
      inputData = values
    }
{-# INLINABLE replaceData #-}

replaceStream :: Stream (Of Logical.Table) m () ->  Input m -> Input m
replaceStream stream input =
  input {
      inputStream = Just stream
    }
{-# INLINABLE replaceStream #-}

isClosed :: Input m -> Bool
isClosed =
  isNothing . inputStream
{-# INLINABLE isClosed #-}

closeStream :: Input m -> Input m
closeStream input =
  input {
      inputStream = Nothing
    }
{-# INLINABLE closeStream #-}

updateInput :: Monad m => Input m -> EitherT UnionTableError m (Input m)
updateInput input =
  case inputStream input of
    Nothing ->
      pure input
    Just stream ->
      if hasData input then
        pure input
      else do
        e <- lift $ Stream.next stream
        case e of
          Left () ->
            pure $
              closeStream input

          Right (hd, tl) -> do
            values <- firstT UnionLogicalSchemaError . hoistEither $ Logical.takeMap hd
            pure . replaceStream tl $ replaceData values input
{-# INLINABLE updateInput #-}

yieldChunked :: Monad m => Map Logical.Value Logical.Value -> Stream (Of Logical.Table) m ()
yieldChunked kvs0 =
  let
    !n =
      Map.size kvs0
  in
    if n == 0 then
      pure ()
    else if n < 4096 then
      Stream.yield $ Logical.Map kvs0
    else do
      let
        -- FIXME use Data.Map.Strict.splitAt when we're able to upgrade to containers >= 0.5.8
        (kvs1, kvs2) =
          bimap Map.fromDistinctAscList Map.fromDistinctAscList .
            List.splitAt 4096 $
            Map.toAscList kvs0

      Stream.yield $ Logical.Map kvs1
      yieldChunked kvs2
{-# INLINABLE yieldChunked #-}

unionStep :: Monad m => Cons Boxed.Vector (Input m) -> EitherT UnionTableError m (Step m)
unionStep inputs = do
  step <- firstT UnionLogicalMergeError . hoistEither . Logical.unionStep $ fmap inputData inputs
  pure $
    Step
      (Logical.unionComplete step)
      (Cons.zipWith replaceData (Logical.unionRemaining step) inputs)
{-# INLINABLE unionStep #-}

unionInput ::
     Monad m
  => Cons Boxed.Vector (Input m)
  -> Stream (Of Logical.Table) (EitherT UnionTableError m) ()
unionInput inputs = do
  let
    loop !_ inputs0 = do
      inputs1 <- lift $ traverse updateInput inputs0
      if Cons.all isClosed inputs1 then do
        pure ()
      else do
        Step values inputs2 <- lift $ unionStep inputs1
        yieldChunked values
        loop SPEC inputs2

  loop SPEC inputs
{-# INLINABLE unionInput #-}

unionLogical ::
     Monad m
  => Schema.Table
  -> Cons Boxed.Vector (Stream (Of Logical.Table) m ())
  -> Stream (Of Logical.Table) (EitherT UnionTableError m) ()
unionLogical schema inputs = do
  Stream.whenEmpty (Logical.empty schema) $
    unionInput $ fmap (Input Map.empty . Just) inputs
{-# INLINABLE unionLogical #-}

unionStriped ::
     Monad m
  => Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStriped inputs0 = do
  (heads, inputs1) <- fmap Cons.unzip . lift $ traverse peekHead inputs0
  schema <- lift . hoistEither . takeSchema $ fmap Striped.schema heads

  let
    fromStriped =
      Stream.mapM (hoistEither . first UnionStripedError . Striped.toLogical) . hoist lift

  hoist squash .
    Stream.mapM (hoistEither . first UnionStripedError . Striped.fromLogical schema) $
    unionLogical schema (fmap fromStriped inputs1)
{-# INLINABLE unionStriped #-}
