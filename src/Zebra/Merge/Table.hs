{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Merge.Table (
    UnionTableError(..)

  , unionList
  , unionFile
  ) where

import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Primitive (PrimMonad)
import           Control.Monad.ST (runST)

import qualified Data.ByteString.Builder as Builder
import           Data.List.NonEmpty (NonEmpty(..))
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as Boxed
import qualified Data.Vector.Mutable as MBoxed

import           P

import           System.IO (FilePath, IOMode(..))
import qualified System.IO as IO

import           X.Control.Monad.Trans.Either (EitherT, runEitherT, hoistEither)
import           X.Data.Vector.Ref (Ref)
import qualified X.Data.Vector.Ref as Ref
import           X.Data.Vector.Stream (Stream(..), SPEC(..))
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Data.Vector.Cons (Cons)
import qualified Zebra.Data.Vector.Cons as Cons
import           Zebra.Schema (TableSchema, SchemaError)
import           Zebra.Serial.Block
import           Zebra.Serial.File
import           Zebra.Serial.Header
import           Zebra.Table (Table, TableError)
import qualified Zebra.Table as Table
import           Zebra.Value (Value, ValueSchemaError, ValueUnionError)
import qualified Zebra.Value as Value

data UnionTableError =
    UnionDecodeError !DecodeError
  | UnionTableError !TableError
  | UnionValueSchemaError !ValueSchemaError
  | UnionValueUnionError !ValueUnionError
  | UnionSchemaMismatch !TableSchema !TableSchema
  | UnionSchemaError !SchemaError
    deriving (Eq, Ord, Show)

data Status =
    Active
  | Complete
    deriving (Eq)

data Input m =
  Input {
      inputSchema :: !TableSchema
    , inputStatus :: !Status
    , inputData :: !(Map Value Value)
    , inputRead :: EitherT UnionTableError m (Maybe Table)
    }

data Output m a =
  Output {
      outputWrite :: Table -> EitherT UnionTableError m ()
    , outputClose :: EitherT UnionTableError m a
    } deriving (Functor)

data Step m =
  Step {
      _stepComplete :: !(Map Value Value)
    , _stepRemaining :: !(Cons Boxed.Vector (Input m))
    }

------------------------------------------------------------------------
-- General

boxed :: m (Ref MBoxed.MVector s a) -> m (Ref MBoxed.MVector s a)
boxed =
  id

takeSchema :: Cons Boxed.Vector (Input m) -> Either UnionTableError TableSchema
takeSchema inputs =
  let
    (schema0, schemas) =
      Cons.uncons $ fmap inputSchema inputs
  in
    case Boxed.find (/= schema0) schemas of
      Nothing ->
        pure schema0
      Just wrong ->
        Left $ UnionSchemaMismatch schema0 wrong

hasData :: Input m -> Bool
hasData =
  not . Map.null . inputData

replaceData :: Input m -> Map Value Value -> Input m
replaceData input values =
  input {
      inputData = values
    }

isComplete :: Input m -> Bool
isComplete =
  (== Complete) . inputStatus

completeInput :: Input m -> Input m
completeInput input =
  input {
      inputStatus = Complete
    , inputData = Map.empty
    }

updateInput :: Monad m => Input m -> EitherT UnionTableError m (Input m)
updateInput input = do
  case inputStatus input of
    Complete ->
      pure input
    Active ->
      if hasData input then
        pure input
      else
        inputRead input >>= \case
          Nothing ->
            pure $ completeInput input

          Just table -> do
            collection <- firstT UnionTableError . hoistEither $ Table.toCollection table
            values <- firstT UnionValueSchemaError . hoistEither $ Value.takeMap collection
            pure $ replaceData input values

writeOutput :: Monad m => Output m a -> Table -> EitherT UnionTableError m ()
writeOutput output table =
  let
    n =
      Table.length table
  in
    if n == 0 then
      pure ()
    else if n < 4096 then
      outputWrite output table
    else do
      let
        (table0, table1) =
          Table.splitAt 4096 table

      outputWrite output table0
      writeOutput output table1

unionStep :: Monad m => Cons Boxed.Vector (Input m) -> EitherT UnionTableError m (Step m)
unionStep inputs = do
  step <- firstT UnionValueUnionError . hoistEither . Value.unionStep $ fmap inputData inputs
  pure $
    Step
      (Value.unionComplete step)
      (Cons.zipWith replaceData inputs (Value.unionRemaining step))

unionLoop :: Monad m => TableSchema -> Cons Boxed.Vector (Input m) -> Output m a -> EitherT UnionTableError m a
unionLoop schema inputs0 output = do
  inputs1 <- traverse updateInput inputs0
  if Cons.all isComplete inputs1 then do
    outputClose output
  else do
    Step values inputs <- unionStep inputs1

    table <- firstT UnionTableError . hoistEither $ Table.fromCollection schema (Value.Map values)
    writeOutput output table

    unionLoop schema inputs output

------------------------------------------------------------------------
-- List

mkListInput :: PrimMonad m => NonEmpty Table -> EitherT UnionTableError m (Input m)
mkListInput = \case
  x :| xs -> do
    ref <- boxed $ Ref.newRef (x : xs)
    pure . Input (Table.schema x) Active Map.empty $ do
      ys0 <- Ref.readRef ref
      case ys0 of
        [] ->
          pure Nothing
        y : ys -> do
          Ref.writeRef ref ys
          pure $ Just y

mkListOutput :: PrimMonad m => EitherT UnionTableError m (Output m [Table])
mkListOutput = do
  ref <- boxed $ Ref.newRef []

  let
    append x =
      Ref.modifyRef ref (<> [x])

    close =
      Ref.readRef ref

  pure $ Output append close

unionList :: Cons Boxed.Vector (NonEmpty Table) -> Either UnionTableError (NonEmpty Table)
unionList inputLists =
  runST $ runEitherT $ do
    inputs <- traverse mkListInput inputLists
    schema <- hoistEither $ takeSchema inputs
    output <- mkListOutput
    result <- unionLoop schema inputs output
    case result of
      [] ->
        pure $ Table.empty schema :| []
      x : xs ->
        pure $ x :| xs

------------------------------------------------------------------------
-- File

mkStreamReader :: MonadIO m => Stream (EitherT x m) b -> EitherT x m (EitherT x m (Maybe b))
mkStreamReader (Stream step sinit) = do
  ref <- liftIO . boxed $ Ref.newRef sinit

  let
    loop _ = do
      s0 <- liftIO $ Ref.readRef ref
      step s0 >>= \case
        Stream.Yield v s -> do
          liftIO $ Ref.writeRef ref s
          pure (Just v)
        Stream.Skip s -> do
          liftIO $ Ref.writeRef ref s
          loop SPEC
        Stream.Done -> do
          pure Nothing

  pure $ loop SPEC

mkFileInput :: MonadIO m => FilePath -> EitherT UnionTableError m (Input m)
mkFileInput path = do
  (schema, stream) <- firstT UnionDecodeError $ fileOfFilePathV3 path
  reader <- firstT UnionDecodeError $ mkStreamReader stream
  pure . Input schema Active Map.empty $
    firstT UnionDecodeError reader

mkFileOutput :: MonadIO m => TableSchema -> FilePath -> EitherT UnionTableError m (Output m ())
mkFileOutput schema path = do
  hout <- liftIO $ IO.openBinaryFile path WriteMode
  liftIO . Builder.hPutBuilder hout . bHeader $ HeaderV3 schema

  pure $ Output
      (liftIO . Builder.hPutBuilder hout . bTableV3)
      (liftIO $ IO.hClose hout)

unionFile :: MonadIO m => Cons Boxed.Vector FilePath -> FilePath -> EitherT UnionTableError m ()
unionFile inputPaths outputPath = do
  inputs <- traverse mkFileInput inputPaths
  schema <- hoistEither $ takeSchema inputs
  output <- mkFileOutput schema outputPath

  unionLoop schema inputs output
